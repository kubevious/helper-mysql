import 'mocha';
import should = require('should');
import _ from 'the-lodash';

import { setupLogger, LoggerOptions } from 'the-logger';
const loggerOptions = new LoggerOptions().enableFile(false).pretty(true);
const logger = setupLogger('test', loggerOptions);

import { MySqlDriver } from '../src';

function buildTestSuite(isDebug : boolean) {

describe('mysql-synchronizer', function() {

    it('synchronizer-test', function() {
        var mysqlDriver = new MySqlDriver(logger, null, isDebug);

        mysqlDriver.onMigrate(() => {
            return mysqlDriver.executeSql(
                "CREATE TABLE IF NOT EXISTS `sync_test` (" +
                    "`id` int unsigned NOT NULL AUTO_INCREMENT," +
                    "`name` varchar(128) NOT NULL," +
                    "`msg` TEXT NOT NULL," +
                    "PRIMARY KEY (`id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=latin1;");
        })

        mysqlDriver.connect();
        var synchronizer = mysqlDriver.synchronizer(logger, 'sync_test', [], ['name', 'msg']);

        return mysqlDriver.waitConnect()
        .then(() => mysqlDriver.executeSql("DELETE FROM sync_test;"))
        .then(() => mysqlDriver.executeSql("SELECT * FROM sync_test;"))
        .then(result => {
            (result).should.be.an.Array();
            (result.length).should.be.equal(0);
        })
        .then(() => {
            return synchronizer.execute({}, [
                { name: 'dog', msg: 'hello'}, 
                { name: 'dog', msg: 'wof-wof'}, 
                { name: 'cat', msg: 'hi'}, 
                { name: 'cat', msg: 'meau'},
            ])
        })
        .then(() => mysqlDriver.executeSql("SELECT * FROM `sync_test`;"))
        .then(result => {
            (result).should.be.an.Array();
            (result.length).should.be.equal(4);
        })
        .then(() => mysqlDriver.executeSql("DELETE FROM `sync_test` WHERE `name` = 'dog';"))
        .then(() => mysqlDriver.executeSql("SELECT * FROM `sync_test`;"))
        .then(result => {
            (result).should.be.an.Array();
            (result.length).should.be.equal(2);
        })
        .then(() => {
            return synchronizer.execute({}, [
                { name: 'dog', msg: 'hello'}, 
                { name: 'dog', msg: 'wof-wof'}, 
                { name: 'cat', msg: 'hi'}, 
                { name: 'cat', msg: 'meau'},
            ])
        })
        .then(() => mysqlDriver.executeSql("SELECT * FROM `sync_test`;"))
        .then(result => {
            (result).should.be.an.Array();
            (result.length).should.be.equal(4);
        })
        .then(() => mysqlDriver.executeSql("INSERT INTO `sync_test`(`name`, `msg`) VALUES('cow', 'muuu')"))
        .then(() => mysqlDriver.executeSql("SELECT * FROM `sync_test`;"))
        .then(result => {
            (result).should.be.an.Array();
            (result.length).should.be.equal(5);
        })
        .then(() => {
            return synchronizer.execute({}, [
                { name: 'dog', msg: 'hellozzz'}, 
                { name: 'dog', msg: 'wof-wof'}, 
                { name: 'cat', msg: 'hi'}, 
                { name: 'cat', msg: 'meau'},
            ])
        })
        .then(() => mysqlDriver.executeSql("SELECT * FROM `sync_test`;"))
        .then(result => {
            (result).should.be.an.Array();
            (result.length).should.be.equal(4);
        })
        .then(() => {
            return mysqlDriver.executeSql("DROP TABLE sync_test;");
        })
        .then(() => {
            return mysqlDriver.close();
        });
    });



});

}

buildTestSuite(true);
buildTestSuite(false);