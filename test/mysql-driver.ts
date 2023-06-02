import 'mocha';
import should from 'should';
import _ from 'the-lodash';

import { setupLogger, LoggerOptions } from 'the-logger';
const loggerOptions = new LoggerOptions().enableFile(false).pretty(true);
const logger = setupLogger('test', loggerOptions);

import { MySqlDriver } from '../src';

function buildTestSuite(isDebug : boolean) {

describe('mysql-driver', function() {

    it('constructor', function() {
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);
        mysqlDriver.close();
    });


    it('connect', function() {
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        mysqlDriver.connect();

        return new Promise<void>((resolve, reject) => {
            mysqlDriver.onConnect(() => {
                resolve();
            })
        })
        .then(() => {
            return mysqlDriver.close();
        });
    });


    it('execute-sql-1', function() {
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        mysqlDriver.connect();

        return mysqlDriver.waitConnect()
        .then(() => {
            return mysqlDriver.executeSql("SELECT table_name FROM information_schema.tables;");
        })
        .then(result => {
            should(result).be.an.Array();
            (result.length > 1).should.be.true();
        })
        .then(() => {
            return mysqlDriver.close();
        });
    });


    it('execute-sql-2', function() {
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

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
            should(result).be.an.Array();
            (result.length == 1).should.be.true();
            (result[0].name).should.be.equal('John Doe');
        })
        .then(() => {
            return mysqlDriver.close();
        });
    });

    it('execute-sql-error', function() {
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        mysqlDriver.connect();

        let returnedError = false;

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
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        mysqlDriver.connect();

        return mysqlDriver.waitConnect()
        .then(() => {

            const deleteStatement = mysqlDriver.statement("DELETE FROM contacts;");
            const insertStatement = mysqlDriver.statement("INSERT INTO contacts(`name`, `email`) VALUES(?, ?);");
            const selectStatement = mysqlDriver.statement("SELECT * FROM contacts;");

            return deleteStatement.execute()
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    should(result).be.an.Array();
                    (result.length == 0).should.be.true();
                })
                .then(() => insertStatement.execute(['John Doe', 'john@doe.com']))
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    should(result).be.an.Array();
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
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        mysqlDriver.connect();

        return mysqlDriver.waitConnect()
        .then(() => {

            const selectStatement = mysqlDriver.statement("SELECT * FROM contactZ;");

            let returedError = false;
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

    it('prepare-statement-after-connect-error-2', function() {
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        mysqlDriver.connect();

        return mysqlDriver.waitConnect()
        .then(() => {

            const selectStatement = mysqlDriver.statement("SELECT * FROM contactZ;");

            let returedError = false;
            return selectStatement.execute()
                .then(() => selectStatement.execute())
                .catch(reason => {
                    returedError = true;
                })
                .then(() => {
                    (returedError).should.be.true();
                    returedError = false;
                })
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
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        const selectStatement = mysqlDriver.statement("SELECT * FROM contacts;");

        let returedError = false;
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
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        const deleteStatement = mysqlDriver.statement("DELETE FROM contacts;");
        const insertStatement = mysqlDriver.statement("INSERT INTO contacts(`name`, `email`) VALUES(?, ?);");
        const selectStatement = mysqlDriver.statement("SELECT * FROM contacts;");
            
        mysqlDriver.connect();
        return mysqlDriver.waitConnect()
        .then(() => {
            return deleteStatement.execute()
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    should(result).be.an.Array();
                    (result.length == 0).should.be.true();
                })
                .then(() => insertStatement.execute(['John Doe', 'john@doe.com']))
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    should(result).be.an.Array();
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
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        const selectStatement = mysqlDriver.statement("SELECT * FROM contactZ;");
            
        let returedError = false;
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



    it('prepare-reconnect', function() {
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        const deleteStatement = mysqlDriver.statement("DELETE FROM contacts;");
        const insertStatement = mysqlDriver.statement("INSERT INTO contacts(`name`, `email`) VALUES(?, ?);");
        const selectStatement = mysqlDriver.statement("SELECT * FROM contacts;");
            
        let returedError = false;

        mysqlDriver.connect();
        return mysqlDriver.waitConnect()
        .then(() => {
            return deleteStatement.execute()
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    should(result).be.an.Array();
                    (result.length == 0).should.be.true();
                })
                .then(() => insertStatement.execute(['John Doe', 'john@doe.com']))
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    should(result).be.an.Array();
                    (result.length == 1).should.be.true();
                    (result[0].name).should.be.equal('John Doe');
                    (result[0].email).should.be.equal('john@doe.com');
                })
                .then(() => mysqlDriver.close())
                .then(() => selectStatement.execute())
                .catch(reason => {
                    logger.info(reason);
                    returedError = true;
                })
                .then(() => {
                    (returedError).should.be.true();
                })
                .then(() => {
                    return mysqlDriver.connect();
                })
                .then(() => mysqlDriver.waitConnect())
                .then(() => selectStatement.execute())
                .then(result => {
                    logger.info("Result: ", result);
                    should(result).be.an.Array();
                    (result.length == 1).should.be.true();
                    (result[0].name).should.be.equal('John Doe');
                    (result[0].email).should.be.equal('john@doe.com');
                })
                ;

        })
        .then(() => {
            return mysqlDriver.close();
        });
    });


    it('some-cleanup', function() {
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        mysqlDriver.connect();

        return mysqlDriver.waitConnect()
        .then(() => {
            return mysqlDriver.executeSql("DROP TABLE IF EXISTS sync_test;");
        })
        .then(() => {
            return mysqlDriver.close();
        });
    });


    it('on-migrate', function() {
        const mysqlDriver = new MySqlDriver(logger, null, isDebug);

        mysqlDriver.onMigrate(() => {
            return mysqlDriver.executeSql(
                "CREATE TABLE `sync_test` (" +
                    "`id` int unsigned NOT NULL AUTO_INCREMENT," +
                    "`name` varchar(128) NOT NULL," +
                    "`msg` TEXT NOT NULL," +
                    "PRIMARY KEY (`id`)" +
                ") ENGINE=InnoDB DEFAULT CHARSET=latin1;");
        })

        mysqlDriver.connect();

        return mysqlDriver.waitConnect()
        .then(() => {
            return mysqlDriver.executeSql("SELECT * FROM sync_test;");
        })
        .then(result => {
            should(result).be.an.Array();
            (result.length).should.be.equal(0);
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