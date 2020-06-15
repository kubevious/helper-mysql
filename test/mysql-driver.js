const should = require('should');
const _ = require('the-lodash');
const logger = require('the-logger').setup('test', { pretty: true });
const MySqlDriver = require('../');

describe('mysql-driver', function() {

    it('constructor', function() {
        var mysqlDriver = new MySqlDriver(logger, true);
        mysqlDriver.close();
    });

    it('connect', function() {
        var mysqlDriver = new MySqlDriver(logger, true);

        mysqlDriver.connect();

        return new Promise((resolve, reject) => {
            mysqlDriver.onConnect(() => {
                resolve();
            })
        })
        .then(() => {
            return mysqlDriver.close();
        });
    });

    it('execute', function() {
        var mysqlDriver = new MySqlDriver(logger, true);

        mysqlDriver.connect();

        return mysqlDriver.waitConnect()
        .then(() => {
            return mysqlDriver.executeSql("SELECT table_name FROM information_schema.tables;");
        })
        .then(result => {
            (result).should.be.an.Array();
            (result.length > 1).should.be.true();
        })
        .then(() => {
            return mysqlDriver.close();
        });
    });

});