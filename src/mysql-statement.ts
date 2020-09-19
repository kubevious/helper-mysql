import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger'
import { HandledError } from './handled-error'

export class MySqlStatement
{
    private _driver : any;
    private logger : ILogger
    private _sql : string;
    private _statement : any;
    private _isPreparing = false;
    private _waiters : any[] = [];

    constructor(logger : ILogger, driver : any, sql : string)
    {
        this._driver = driver;
        this.logger = logger;
        this._sql = sql;
        this._statement = null;
        this._isPreparing = false;
        this._waiters = [];
    }

    get isDebug() {
        return this._driver._isDebug;
    }

    get isConnected() {
        return this._driver.isConnected;
    }

    get isPrepared() {
        return _.isNotNullOrUndefined(this._statement);
    }

    reset() {
        this._statement = null;
        this._isPreparing = false;
    }

    execute(params : any[])
    {
        this.logger.silly("[_execute] executing: %s", this._sql);
        if (this.isDebug) {
            this.logger.info("[_execute] executing: %s", this._sql, params);
        }

        if (this.isPrepared)
        {
            return this._execute(params);
        }

        return this.prepare()
            .then(() => this._execute(params))
            ;
    }

    _execute(params : any[]) : Promise<any>
    {
        if (!this.isConnected) {
            return Promise.reject('NotConnected.');
        }

        if (!this._statement) {
            return Promise.reject('NotPrepared.');
        }

        return Promise.construct<any>((resolve, reject) => {
            params = this._driver._massageParams(params);

            this._statement.execute(params, (err, results, fields) => {
                if (err) {
                    this.logger.error("[_execute] ERROR.", this._sql, err);
                    reject(err);
                    return;
                }
                if (this.isDebug) {
                    this.logger.info("[_execute] DONE.", this._sql, results);
                }
                resolve(results);
            });
        });
    }

    prepare() : Promise<void>
    {
        if (this._statement) {
            return Promise.resolve();
        }

        if (this._isPreparing) {
            return new Promise((resolve, reject) => {
                this._waiters.push({
                    resolve,
                    reject
                })
            });
        }
        this._isPreparing = true;
        this.logger.info('[prepare] BEGIN: %s', this._sql);

        return new Promise((resolve, reject) => {
            this._waiters.push({
                resolve: resolve,
                reject: reject
            });

            if (!this.isConnected) {
                this._handlePrepareError('Not Connected', new HandledError("Not Connected"));
                return; 
            }
            this._driver._connection.prepare(this._sql, (err, statement) => {                
                if (err)
                {
                    this._handlePrepareError('Failed to prepare', err);
                    return;
                }

                this.logger.info('[prepare] prepared: %s. inner id: %s', this._sql, statement.id);
                this._isPreparing = false;
                this._statement = statement;

                for(var x of this._waiters)
                {
                    x.resolve();
                }
                this._waiters = [];
            });
        });
    }

    _handlePrepareError(message: string, error : any)
    {
        this.logger.error('[_handlePrepareError] failed to prepare statement. %s', message, this._sql, error);
        this._statement = null;
        this._isPreparing = false;
        for(var x of this._waiters)
        {
            x.reject(error);
        }
        this._waiters = [];
    }


}