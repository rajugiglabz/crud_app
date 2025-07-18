const mysql = require('mysql2');
const dotenv = require('dotenv');
const redis = require('redis');
const { v4: uuidv4 } = require('uuid');
const cron = require('node-cron');

let instance = null;
dotenv.config();

const connection = mysql.createConnection({
    host: process.env.HOST,
    user: process.env.USER,
    password: process.env.PASSWORD,
    database: process.env.DATABASE,
    port: process.env.DB_PORT
});

connection.connect((err) => {
    if (err) {
        console.log(err.message);
    }
     console.log('db ' + connection.state);
});

const redisClient = redis.createClient({
    socket: {
        host: process.env.REDIS_HOST || '127.0.0.1',
        port: process.env.REDIS_PORT || 6379
    }
});

redisClient.connect().catch(console.error);


class DbService {
    static getDbServiceInstance() {
        return instance ? instance : new DbService();
    }

    cacheKey = 'users:all';


    async getAllData() {
        try {
            const cacheResults = await redisClient.get(this.cacheKey);

            if(cacheResults){
                console.log('Received data from cache')
                return JSON.parse(cacheResults);
            }

            console.log("data didn't received from redis");

            const response = await new Promise((resolve, reject) => {
                const query = "SELECT * FROM users;";

                connection.query(query, (err, results) => {
                    if (err) reject(new Error(err.message));
                    resolve(results);
                })
            });
            // console.log(response);

            await redisClient.SETEX(this.cacheKey,3600,JSON.stringify(response));

            return response;
        } catch (error) {
            console.log(error);
        }
    }


    async insertNewName(name) {
        try {
            const dateAdded = new Date();

            const newUser = {
                uuid : uuidv4(),
                name,
                date_added : dateAdded
            }
            //const insertId = await new Promise((resolve, reject) => {
             //   const query = "INSERT INTO users (name, date_added) VALUES (?,?);";

                await redisClient.rPush('users:pending',JSON.stringify(newUser))
                //connection.query(query, [name, dateAdded] , (err, result) => {
                  //  if (err) reject(new Error(err.message));
                   // resolve(result.insertId);
                //})
            //});

            //await redisClient.del(this.cacheKey);

            return newUser;
        } catch (error) {
            console.log(error);
        }
    }

    async deleteRowById(id) {
        try {
            id = parseInt(id, 10); 
            const response = await new Promise((resolve, reject) => {
                const query = "DELETE FROM users WHERE id = ?";
    
                connection.query(query, [id] , (err, result) => {
                    if (err) reject(new Error(err.message));
                    resolve(result.affectedRows);
                })
            });

            await redisClient.del(this.cacheKey);
    
            return response === 1 ? true : false;
        } catch (error) {
            console.log(error);
            return false;
        }
    }

    async updateNameById(id, name) {
        try {
            id = parseInt(id, 10); 
            const response = await new Promise((resolve, reject) => {
                const query = "UPDATE users SET name = ? WHERE id = ?";
    
                connection.query(query, [name, id] , (err, result) => {
                    if (err) reject(new Error(err.message));
                    resolve(result.affectedRows);
                })
            });

            await redisClient.del(this.cacheKey);
    
            return response === 1 ? true : false;
        } catch (error) {
            console.log(error);
            return false;
        }
    }

    async searchByName(name) {
        try {
            const response = await new Promise((resolve, reject) => {
                const query = "SELECT * FROM users WHERE name = ?;";

                connection.query(query, [name], (err, results) => {
                    if (err) reject(new Error(err.message));
                    resolve(results);
                })
            });

            return response;
        } catch (error) {
            console.log(error);
        }
    }
}

cron.schedule( "* * * * *",
  async () => {
    await writeCrudDataToDBFromRedis();
  }
);

async function writeCrudDataToDBFromRedis() {
    try {
        const length = await redisClient.lLen('users:pending');
        if (length === 0) return;

        const userStrings = await redisClient.lRange('users:pending', 0, length - 1);
        const users = [];

        for(const str of userStrings){
            const user = JSON.parse(str);
            users.push(user);
        }

        const values = [];
        for(const user of users){
            values.push([user.name,new Date(user.date_added)])
        }

        const query = "INSERT INTO users (name, date_added) VALUES ?";
        
        await new Promise((resolve, reject) => {
            connection.query(query, [values], (err, result) => {
                if (err) reject(err);
                resolve(result);
            });
        });

    
        await redisClient.lTrim('users:pending', length, -1);

        await redisClient.del('users:pending');

        console.log(`Flushed ${users.length} users to DB`);
    } catch (error) {
        console.error("Error during flush:", error);
    }
}

module.exports = DbService;