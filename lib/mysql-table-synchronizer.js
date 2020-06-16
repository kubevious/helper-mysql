const _ = require('the-lodash');
const HashUtils = require('./hash-utils');

class MySqlTableSynchronizer
{
    constructor(logger, driver, table, filterFields, syncFields)
    {
        this._logger = logger.sublogger('TableSynchronizer');
        this._driver = driver;
        this._table = table;
        this._filterFields = filterFields || [];
        this._syncFields = syncFields || [];
        this._skipDelete = false;

        this._prepareQueryStatement();
        this._prepareCreateStatement();
        this._prepareDeleteStatement();
    }

    get logger() {
        return this._logger;
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
        var fields = [];
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

    execute(filterValues, items)
    {
        return this._queryCurrent(filterValues)
            .then(currentItems => {

                var currentItemsDict = {}
                for(var item of currentItems)
                {
                    var id = item.id;
                    delete item.id;
                    currentItemsDict[HashUtils.calculateObjectHashStr(item)] = {
                        id: id,
                        item: item
                    }
                }

                var targetItemsDict = {}
                for(var item of items)
                {
                    targetItemsDict[HashUtils.calculateObjectHashStr(item)] = item;
                }

                return this._productDelta(currentItemsDict, targetItemsDict);
            })
            .then(delta => {
                return this._executeDelta(delta);
            })
    }

    _queryCurrent(filterValues)
    {
        var params = this._filterFields.map(x => filterValues[x]);
        return this._queryStatement.execute(params)
    }

    _productDelta(currentItemsDict, targetItemsDict)
    {
        var delta = [];

        if (!this._skipDelete) {
            for(var h of _.keys(currentItemsDict))
            {
                if (!targetItemsDict[h]) {
                    delta.push({
                        action: 'D',
                        id: currentItemsDict[h].id
                    });
                }
            }
        }

        for(var h of _.keys(targetItemsDict))
        {
            if (!currentItemsDict[h]) {
                delta.push({
                    action: 'C',
                    item: targetItemsDict[h]
                });
            }
        }

        return delta;
    }

    _executeDelta(delta)
    {
        var statements = delta.map(delta => {
            var statement = null;
            var params = null;

            if (delta.action == 'C') {
                statement = this._createStatement;
                var params = this._filterFields.map(x => delta.item[x]);
                params = _.concat(params, this._syncFields.map(x => delta.item[x]))
            } else if (delta.action == 'D') {
                statement = this._deleteStatement;
                params = [delta.id];
            }

            return {
                statement, 
                params
            }
        });

        return this._driver.executeStatements(statements);
    }

}

module.exports = MySqlTableSynchronizer;