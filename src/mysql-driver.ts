import _ from 'the-lodash';
import { ILogger } from 'the-logger'
import { HandledError } from './handled-error'
import { createConnection, Connection, ConnectionOptions } from 'mysql2';
import { EventEmitter } from 'events'
import { MySqlStatement } from './mysql-statement'
import { MySqlTableSynchronizer } from './mysql-table-synchronizer'
import { massageParams } from './utils'
import { PartitionManager } from './partition-manager';

import dotenv from 'dotenv';
import { MyPromise } from 'the-promise';
dotenv.config();

export interface StatementInfo 
{
    statement : MySqlStatement;
    params: any[];
}

export type ConnectCallback = ((driver : MySqlDriver) => Promise<any> | void);
export type MigrateCallback = ((driver : MySqlDriver) => Promise<any>);
export type TxCallback = ((driver : MySqlDriver) => Promise<any>);

export class MySqlDriver
{
    private logger : ILogger
    private _statements : Record<string, MySqlStatement> = {};
    private _migrators : MigrateCallback[] = [];
    private _connectEmitter = new EventEmitter();
    private _isDebug = false;
    private _isClosed = false;
    private _isConnecting = false;
    private _isConnected = false;
    private _connection? : Connection;
    private _mysqlConnectParams : ConnectionOptions;
    private _partitionManager : PartitionManager;

    constructor(logger: ILogger, params : ConnectionOptions | null, isDebug? : boolean)
    {
        this.logger = logger.sublogger("MySqlDriver");
        this._statements = {};
        this._migrators = [];
        this._isDebug = isDebug || false;
        this._isClosed = false;

        params = params || {}
        params = _.clone(params);

        this._mysqlConnectParams = <ConnectionOptions>_.defaults(params, {
            host: process.env.MYSQL_HOST,
            port: process.env.MYSQL_PORT,
            database: process.env.MYSQL_DB,
            user: process.env.MYSQL_USER,
            password: process.env.MYSQL_PASS,
            timezone: 'Z',
            charset: 'utf8mb4_general_ci'
        });

        this._partitionManager = new PartitionManager(this.logger, this);
    }

    get isConnected() : boolean{
        return _.isNotNullOrUndefined(this._connection);
    }
    
    get isDebug()  : boolean {
        return this._isDebug;
    }

    get connection() : any {
        return this._connection;
    }

    get partitionManager() {
        return this._partitionManager;
    }

    get databaseName() {
        return this._mysqlConnectParams.database;
    }

    connect() : void
    {
        this._isClosed = false;
        return this._tryConnect();
    }

    close() : void
    {
        this.logger.info('[close]')
        this._isClosed = true;
        this._disconnect();
    }

    onConnect(cb : ConnectCallback)
    {
        if (this.isConnected) {
            this._triggerCallback(cb);
        }
        this._connectEmitter.on('connect', () => {
            this._triggerCallback(cb);
        })
    }

    waitConnect() : Promise<void>
    {
        if (this._isConnected) {
            return Promise.resolve()
        }

        return MyPromise.construct<void>((resolve, reject) => {
            this.onConnect(() => {
                resolve();
            })
        });
    }

    onMigrate(cb : MigrateCallback)
    {
        this._migrators.push(cb);
    }

    statement(sql : string) : MySqlStatement
    {
        if (sql in this._statements) {
            return this._statements[sql];
        }
        const statement = new MySqlStatement(this.logger, this, sql);
        this._statements[sql] = statement;
        return statement;
    }

    prepareStatements() : Promise<void>
    {
        this.logger.info('[prepareStatements] begin')
        const statements = _.values(this._statements).filter(x => !x.isPrepared);
        return MyPromise.serial(statements, x => {
            return x.prepare()
                .catch((reason: any) => {
                    this.logger.error('[prepareStatements] Failed: ', reason);
                });
        })
        .then(() => {
            this.logger.info('[prepareStatements] end')
        });
    }

    executeSql(sql : string, params? : any[]) : Promise<any>
    {
        return MyPromise.construct<any[]>((resolve, reject) => {
            this.logger.silly("[executeSql] executing: %s", sql);

            if (this._isDebug) {
                this.logger.info("[executeSql] executing: %s", sql, params);
            }

            if (!this._connection) {
                reject(new HandledError("NOT CONNECTED"));
                return;
            }
            
            const finalParams = massageParams(params);

            this._connection.execute(sql, finalParams, (err: any, results: any, fields) => {
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

    synchronizer(logger : ILogger, table : string, filterFields : string[], syncFields : string[])
    {
        const synchronizer = new MySqlTableSynchronizer(logger,
                                                        this,
                                                        table,
                                                        filterFields,
                                                        syncFields);
        return synchronizer;
    }


    executeStatements(statementInfos : StatementInfo[]) : Promise<any[]>
    {
        this.logger.info("[executeStatements] BEGIN. Count: %s", statementInfos.length);

        if (this._isDebug)
        {
            return MyPromise.serial(statementInfos, x => {
                this.logger.info("[executeStatements] exec:");
                return x.statement.execute(x.params);
            });
        }
        else
        {
            return MyPromise.parallel(statementInfos, x => {
                return x.statement.execute(x.params);
            });
        }
    }

    executeInTransaction(cb : TxCallback) : Promise<void>
    {
        this.logger.info("[executeInTransaction] BEGIN");

        if (!this._connection) {
            return Promise.reject(new HandledError("NOT CONNECTED"));
        }

        const connection = this._connection!;
        return MyPromise.construct<void>((resolve, reject) => {
            this.logger.info("[executeInTransaction] TX Started.");

            if (!connection) {
                reject(new HandledError("NOT CONNECTED"));
                return;
            }

            const rollback = (err : any) =>
            {
                this.logger.error("[executeInTransaction] Rolling Back.");
                connection.rollback(() => {
                    this.logger.error("[executeInTransaction] Rollback complete.");
                    reject(err);
                });
            }

            connection.beginTransaction((err : any) => {
                if (err) { 
                    reject(err);
                    return;
                }

                return Promise.resolve()
                    .then(() => cb(this))
                    .then(() => {
                        connection.commit((err : any) => {
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

    private _tryConnect()
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
    
            const connection = createConnection(this._mysqlConnectParams);

            connection.on('error', (err : any) => {
                this.logger.error('[_tryConnect] ON ERROR: %s, Message: %s', err.code, err.sqlMessage);
                connection.destroy();
                this._disconnect();
            });
    
            connection.connect((err : any) => {
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
        catch(err : any)
        {
            this._isConnecting = false;
            this._disconnect();
        }
    }

    private _disconnect()
    {
        this.logger.info("[_disconnect]");
        if (this._connection) {
            this._connection.destroy();
        }
        this._connection = undefined;
        for(const x of _.values(this._statements))
        {
            x.reset();
        }
        this._tryReconnect();
    }

    private _acceptConnection(connection : Connection) : Promise<void>
    {
        this._connection = connection;

        return Promise.resolve()
            .then(() => MyPromise.serial(this._migrators, x => x(this)))
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

    private _tryReconnect()
    {
        if (this._isClosed) {
            return;
        }
        setTimeout(this._tryConnect.bind(this), 1000);
    }

    private _triggerCallback(cb : ConnectCallback)
    {
        try
        {
            this.logger.info("[_triggerCallback]")

            setImmediate(() => {
                try
                {
                    const res = cb(this);
                    return Promise.resolve(res)
                        .then(() => null)
                        .catch(reason => {
                            this.logger.error("[_triggerCallback] Promise Failure: ", reason)
                        })
                    }
                    catch(error)
                    {
                        this.logger.error("[_triggerCallback] Exception: ", error);
                    }
            });
        }
        catch(error: any)
        {
            this.logger.error("[_triggerCallback] Exception2: ", error)
        }
    }
}