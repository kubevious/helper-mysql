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

    it('execute-sql-1', function() {
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

    it('execute-sql-2', function() {
        var mysqlDriver = new MySqlDriver(logger, true);

        mysqlDriver.connect();

        return mysqlDriver.waitConnect()
        .then(() => {
            return mysqlDriver.executeSql("DELETE FROM contacts;");
        })
        .then(() => {
            return mysqlDriver.executeSql("INSERT INTO contacts(name, email) VALUES('John Doe', 'john@doe.co');");
        })
        .then(result => {
            logger.info("Result: ", result);
        })
        .then(() => {
            return mysqlDriver.executeSql("SELECT * FROM contacts;");
        })
        .then(result => {
            logger.info("Result: ", result);
            (result).should.be.an.Array();
            (result.length == 1).should.be.true();
            (result[0].name).should.be.equal('John Doe');
        })
        .then(() => {
            return mysqlDriver.close();
        });
    });

});