import _ from 'the-lodash';
import { Promise } from 'the-promise';
import { ILogger } from 'the-logger'
import { calculateObjectHashStr } from './hash-utils'
import { MySqlDriver, StatementInfo } from './mysql-driver'
import { MySqlStatement } from './mysql-statement'

class DeltaAction 
{
    public shouldCreate : boolean;
    public item: object;

    constructor(shouldCreate: boolean, item: object)
    {
        this.shouldCreate = shouldCreate;
        this.item = item;
    }
}

export class MySqlTableSynchronizer
{
    private _driver : MySqlDriver;
    private logger : ILogger
    private _table : string ;
    private _filterFields : string[];
    private _syncFields : string[];
    private _skipDelete = false;

    private _queryStatement? : MySqlStatement;
    private _createStatement? : MySqlStatement;
    private _deleteStatement? : MySqlStatement;

    constructor(logger : ILogger, driver : MySqlDriver, table: string, filterFields : string[], syncFields : string[])
    {
        this.logger = logger;
        this._driver = driver;
        this._table = table;
        this._filterFields = filterFields || [];
        this._syncFields = syncFields || [];
        this._skipDelete = false;

        this._prepareQueryStatement();
        this._prepareCreateStatement();
        this._prepareDeleteStatement();
    }

    markSkipDelete() {
        this._skipDelete = true;
    }

    _prepareQueryStatement()
    {
        var whereClause = '';
        if (this._filterFields.length > 0)
        {
            whereClause = ' WHERE ' + 
            this._filterFields.map(x => '`' + x + '` = ?').join(' AND ');
        }

        var fields = ['id'];
        fields = _.concat(fields, this._filterFields);
        fields = _.concat(fields, this._syncFields);
        fields = fields.map(x => '`' + x + '`');

        var sql = 'SELECT `id` ' + 
            fields.join(', ') +
            ' FROM `' + this._table + '`' +
            whereClause + 
            ';'
            ;

        this._queryStatement = this._driver.statement(sql);
    }

    _prepareCreateStatement()
    {
        var fields : string[] = [];
        fields = _.concat(fields, this._filterFields);
        fields = _.concat(fields, this._syncFields);
        fields = fields.map(x => '`' + x + '`');

        var sql = 'INSERT INTO ' + 
            '`' + this._table + '` (' +
            fields.join(', ') +
            ') VALUES (' + 
            fields.map(x => '?').join(', ') +
            ');';

        this._createStatement = this._driver.statement(sql);
    }

    _prepareDeleteStatement()
    {
        var sql = 'DELETE FROM ' + 
            '`' + this._table + '` ' +
            'WHERE `id` = ?;';

        this._deleteStatement = this._driver.statement(sql);
    }

    execute(filterValues : object, items : object[])
    {
        return this._queryCurrent(filterValues)
            .then(currentItems => {

                var currentItemsDict : Record<string, any> = {}
                for(var item of currentItems)
                {
                    var id = item.id;
                    delete item.id;
                    currentItemsDict[calculateObjectHashStr(item)] = {
                        id: id,
                        item: item
                    }
                }

                var targetItemsDict : Record<string, any> = {}
                for(let item of items)
                {
                    targetItemsDict[calculateObjectHashStr(item)] = item;
                }

                return this._productDelta(currentItemsDict, targetItemsDict);
            })
            .then(delta => {
                return this._executeDelta(delta);
            })
    }

    _queryCurrent(filterValues : object)
    {
        var params = this._filterFields.map(x => _.get(filterValues, x));
        return this._queryStatement!.execute(params)
    }

    _productDelta(currentItemsDict : Record<string, any>, targetItemsDict : Record<string, any>) : DeltaAction[]
    {
        var delta = [];

        if (!this._skipDelete) {
            for(var h of _.keys(currentItemsDict))
            {
                if (!targetItemsDict[h]) {
                    delta.push(
                        new DeltaAction(false, currentItemsDict[h].id)
                    );
                }
            }
        }

        for(var h of _.keys(targetItemsDict))
        {
            if (!currentItemsDict[h]) {
                delta.push(
                    new DeltaAction(true, targetItemsDict[h])
                );
            }
        }

        return delta;
    }

    _executeDelta(delta : DeltaAction[])
    {
        let statements = delta.map(delta => {
            var statement = null;
            var params = null;

            if (delta.shouldCreate) {
                statement = this._createStatement;
                params = this._filterFields.map(x => _.get(delta.item, x));
                params = _.concat(params, this._syncFields.map(x => _.get(delta.item, x)))
            } else {
                statement = this._deleteStatement;
                params = [delta.item];
            }

            return <StatementInfo> {
                statement, 
                params
            }
        });

        return this._driver.executeStatements(statements);
    }
}