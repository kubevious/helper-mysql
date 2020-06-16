const Promise = require('the-promise');
const _ = require('the-lodash');
const mysql = require('mysql2');
const events = require('events');
const HandledError = require('./handled-error');
const MySqlStatement = require('./mysql-statement');

class MySqlDriver
{
    constructor(logger, isDebug)
    {
        this._logger = logger.sublogger("MySqlDriver");
        this._statements = {};
        this._migrators = [];
        this._connectEmitter = new events.EventEmitter();
        this._isDebug = isDebug;
        this._isClosed = false;

        this._mysqlConnectParams = {
            host: process.env.MYSQL_HOST,
            port: process.env.MYSQL_PORT,
            database: process.env.MYSQL_DB,
            user: process.env.MYSQL_USER,
            password: process.env.MYSQL_PASS,
            timezone: 'Z'
        };
    }

    get logger() {
        return this._logger;
    }

    get isConnected() {
        return _.isNotNullOrUndefined(this._connection);
    }

    connect()
    {
       return this._tryConnect();
    }

    close()
    {
        this._logger.info('[close]')
        this._isClosed = true;
        this._disconnect();
    }

    onConnect(cb)
    {
        if (this.isConnected) {
            this._triggerCallback(cb);
        }
        this._connectEmitter.on('connect', () => {
            this._triggerCallback(cb);
        })
    }

    waitConnect()
    {
        if (this._isConnected) {
            return Promise.resolve()
        }

        return new Promise((resolve, reject) => {
            this.onConnect(() => {
                resolve();
            })
        });
    }

    onMigrate(cb)
    {
        this._migrators.push(cb);
    }

    statement(sql)
    {
        if (sql in this._statements) {
            return this._statements[sql];
        }
        var statement = new MySqlStatement(this, sql);
        this._statements[sql] = statement;
        return statement;
    }

    prepareStatements()
    {
        this._logger.info('[prepareStatements] begin')
        var statements = _.values(this._statements).filter(x => !x.isPrepared);
        return Promise.serial(statements, x => {
            return x.prepare()
                .catch(reason => {
                    this.logger.error('[prepareStatements] Failed: ', reason);
                });
        })
        .then(() => {
            this._logger.info('[prepareStatements] end')
        });
    }

    executeSql(sql, params)
    {
        return new Promise((resolve, reject) => {
            this.logger.silly("[executeSql] executing: %s", sql);

            if (this._isDebug) {
                this.logger.info("[executeSql] executing: %s", sql, params);
            }

            if (!this._connection) {
                reject(new HandledError("NOT CONNECTED"));
                return;
            }
            
            params = this._massageParams(params);

            this._connection.execute(sql, params, (err, results, fields) => {
                if (err) {
                    this.logger.error("[executeSql] ERROR IN \"%s\". ", sql, err);
                    reject(err);
                    return;
                }
                // this.logger.info("[executeSql] DONE: %s", sql, results);
                resolve(results);
            });
        });
    }

    _massageParams(params)
    {
        if (!params) {
            params = []
        } else {
            params = params.map(x => {
                if (_.isUndefined(x)) {
                    return null;
                }
                if (_.isPlainObject(x) || _.isArray(x)) {
                    return _.stableStringify(x);
                }
                return x;
            })
        }
        if (this._isDebug) {
            this.logger.info("[_massageParams] final params: ", params);
        }
        return params;
    }

    executeStatements(statements)
    {
        this.logger.info("[executeStatements] BEGIN. Count: %s", statements.length);

        if (this._isDebug)
        {
            return Promise.serial(statements, statement => {
                this.logger.info("[executeStatements] exec:");
                return this.executeStatement(statement.id, statement.params);
            });
        }
        else
        {
            return Promise.parallel(statements, statement => {
                return this.executeStatement(statement.id, statement.params);
            });
        }
    }

    executeInTransaction(cb)
    {
        this.logger.info("[executeInTransaction] BEGIN");

        var connection = this._connection;
        return new Promise((resolve, reject) => {
            this.logger.info("[executeInTransaction] TX Started.");

            if (!connection) {
                reject(new HandledError("NOT CONNECTED"));
                return;
            }

            var rollback = (err) =>
            {
                this.logger.error("[executeInTransaction] Rolling Back.");
                connection.rollback(() => {
                    this.logger.error("[executeInTransaction] Rollback complete.");
                    reject(err);
                });
            }

            connection.beginTransaction((err) => {
                if (err) { 
                    reject(err);
                    return;
                }

                return Promise.resolve()
                    .then(() => cb(this))
                    .then(() => {
                        connection.commit((err) => {
                            if (err) {
                                this.logger.error("[executeInTransaction] TX Failed To Commit.");
                                rollback(err);
                            } else {
                                this.logger.info("[executeInTransaction] TX Completed.");
                                resolve();
                            }
                        });
                    })
                    .catch(reason => {
                        this.logger.error("[executeInTransaction] TX Failed.");
                        rollback(reason);
                    });
            });
        });

    }

    /** IMPL **/

    _tryConnect()
    {
        try
        {
            if (this._connection) {
                return;
            }
            if (this._isConnecting) {
                return;
            }
            this._isConnecting = true;
    
            var connection = mysql.createConnection(this._mysqlConnectParams);

            connection.on('error', (err) => {
                this.logger.error('[_tryConnect] ON ERROR: %s', err.code);
                connection.destroy();
                this._disconnect();
            });
    
            connection.connect((err) => {
                this._isConnecting = false;
    
                if (err) {
                    // this.logger.error('[_tryConnect] CODE=%s', err.code);
                    // this._disconnect();
                    return;
                }
               
                this.logger.info('[_tryConnect] connected as id: %s', connection.threadId);
                this._acceptConnection(connection);
            });
        }
        catch(err)
        {
            this._isConnecting = false;
            this._disconnect();
        }
    }

    _disconnect()
    {
        this._logger.info("[_disconnect]");
        if (this._connection) {
            this._connection.destroy();
        }
        this._connection = null;
        for(var x of _.values(this._statements))
        {
            x.reset();
        }
        this._tryReconnect();
    }

    _acceptConnection(connection)
    {
        this._connection = connection;

        return Promise.resolve()
            .then(() => Promise.serial(this._migrators, x => x(this)))
            .then(() => this.prepareStatements())
            .then(() => {
                this._connectEmitter.emit('connect');
            })
            .catch(reason => {
                if (reason instanceof HandledError) {
                    this.logger.error('[_acceptConnection] failed: %s', reason.message);
                } else {
                    this.logger.error('[_acceptConnection] failed: ', reason);
                }
                this._disconnect();
            })
        ;
    }

    _tryReconnect()
    {
        if (this._isClosed) {
            return;
        }
        setTimeout(this._tryConnect.bind(this), 1000);
    }

    _triggerCallback(cb)
    {
        try
        {
            this._logger.info("[_triggerCallback]")

            setImmediate(() => {
                try
                {
                    var res = cb(this);
                    return Promise.resolve(res)
                        .then(() => {})
                        .catch(reason => {
                            this._logger.error("[_triggerCallback] Promise Failure: ", reason)
                        })
                    }
                    catch(error)
                    {
                        this._logger.error("[_triggerCallback] Exception: ", error);
                    }
            });
        }
        catch(error)
        {
            this._logger.error("[_triggerCallback] Exception2: ", error)
        }
    }
}

module.exports = MySqlDriver;