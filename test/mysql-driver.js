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

    it('execute-sql-error', function() {
        var mysqlDriver = new MySqlDriver(logger, true);

        mysqlDriver.connect();

        var returnedError = false;

        return mysqlDriver.waitConnect()
        .then(() => {
            return mysqlDriver.executeSql("SELECT * contactZ;");
        })
        .catch(error => {
            returnedError = true;
        })
        .then(() => {
            (returnedError).should.be.true();
        })
        .then(() => {
            return mysqlDriver.close();
        });
    });

    it('prepare-statement-after-connect', function() {
        var mysqlDriver = new MySqlDriver(logger, true);

        mysqlDriver.connect();

        return mysqlDriver.waitConnect()
        .then(() => {

            var deleteStatement = mysqlDriver.statement("DELETE FROM contacts;");
            var insertStatement = mysqlDriver.statement("INSERT INTO contacts(`name`, `email`) VALUES(?, ?);");
            var selectStatement = mysqlDriver.statement("SELECT * FROM contacts;");

            return deleteStatement.execute()
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    (result).should.be.an.Array();
                    (result.length == 0).should.be.true();
                })
                .then(() => insertStatement.execute(['John Doe', 'john@doe.com']))
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    (result).should.be.an.Array();
                    (result.length == 1).should.be.true();
                    (result[0].name).should.be.equal('John Doe');
                    (result[0].email).should.be.equal('john@doe.com');
                });

        })
        .then(() => {
            return mysqlDriver.close();
        });
    });

    it('prepare-statement-after-connect-error', function() {
        var mysqlDriver = new MySqlDriver(logger, true);

        mysqlDriver.connect();

        return mysqlDriver.waitConnect()
        .then(() => {

            var selectStatement = mysqlDriver.statement("SELECT * FROM contactZ;");

            var returedError = false;
            return selectStatement.execute()
                .then(() => selectStatement.execute())
                .catch(reason => {
                    returedError = true;
                })
                .then(() => {
                    (returedError).should.be.true();
                });

        })
        .then(() => {
            return mysqlDriver.close();
        });
    });


    it('prepare-statement-no-connect-error', function() {
        var mysqlDriver = new MySqlDriver(logger, true);

        var selectStatement = mysqlDriver.statement("SELECT * FROM contacts;");

        var returedError = false;
        return selectStatement.execute()
            .then(() => selectStatement.execute())
            .catch(reason => {
                logger.info(reason);
                returedError = true;
            })
            .then(() => {
                (returedError).should.be.true();
            })
            .then(() => {
                return mysqlDriver.close();
            });
    });


    it('prepare-statement-before-connect', function() {
        var mysqlDriver = new MySqlDriver(logger, true);

        var deleteStatement = mysqlDriver.statement("DELETE FROM contacts;");
        var insertStatement = mysqlDriver.statement("INSERT INTO contacts(`name`, `email`) VALUES(?, ?);");
        var selectStatement = mysqlDriver.statement("SELECT * FROM contacts;");
            
        mysqlDriver.connect();
        return mysqlDriver.waitConnect()
        .then(() => {
            return deleteStatement.execute()
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    (result).should.be.an.Array();
                    (result.length == 0).should.be.true();
                })
                .then(() => insertStatement.execute(['John Doe', 'john@doe.com']))
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    (result).should.be.an.Array();
                    (result.length == 1).should.be.true();
                    (result[0].name).should.be.equal('John Doe');
                    (result[0].email).should.be.equal('john@doe.com');
                });

        })
        .then(() => {
            return mysqlDriver.close();
        });
    });


    it('prepare-statement-before-connect-error', function() {
        var mysqlDriver = new MySqlDriver(logger, true);

        var selectStatement = mysqlDriver.statement("SELECT * FROM contactZ;");
            
        var returedError = false;
        mysqlDriver.connect();
        return mysqlDriver.waitConnect()
            .then(() => {
                return selectStatement.execute()
                    .catch(reason => {
                        logger.info(reason);
                        returedError = true;
                    })
                    .then(() => {
                        (returedError).should.be.true();
                    })
            })
            .then(() => {
                return mysqlDriver.close();
            });
    });

});