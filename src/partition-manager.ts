import _ from 'the-lodash';
import { ILogger } from 'the-logger'

import { MySqlDriver } from './mysql-driver';
export class PartitionManager
{
    private _logger : ILogger
    private _driver: MySqlDriver

    constructor(logger: ILogger, driver: MySqlDriver)
    {
        this._logger = logger;
        this._driver = driver;
    }

    queryPartitions(tableName: string)
    {
        const sql = 
            "SELECT PARTITION_NAME, PARTITION_DESCRIPTION " +
            "FROM information_schema.partitions " +
            `WHERE TABLE_SCHEMA='${this._driver.databaseName}' ` +
            `AND TABLE_NAME = '${tableName}' ` +
            'AND PARTITION_NAME IS NOT NULL ' +
            'AND PARTITION_DESCRIPTION != 0;';
        
        return this._driver.executeSql(sql)
            .then((results: any[]) => {
                return results.map(x => ({
                    name: <string>x.PARTITION_NAME,
                    value: parseInt(x.PARTITION_DESCRIPTION)
                }));
            })
    }

    createPartition(tableName: string, name: string, value: number)
    {
        this._logger.info("[createPartition] Table: %s, %s -> %s", tableName, name, value);

        const sql = 
            `ALTER TABLE \`${tableName}\` ` +
            `ADD PARTITION (PARTITION ${name} VALUES LESS THAN (${value}))`;
        
        return this._driver.executeSql(sql);
    }

    dropPartition(tableName: string, name: string)
    {
        this._logger.info("[dropPartition] Table: %s, %s", tableName, name);

        const sql = 
            `ALTER TABLE \`${tableName}\` ` +
            `DROP PARTITION ${name}`;
        
        return this._driver.executeSql(sql);
    }

}