const AWS = require("aws-sdk");
AWS.config.update({ region: 'eu-west-1' });

const _ = require("underscore");
const moment = require("moment");

const dynamodb = new AWS.DynamoDB({
    apiVersion: '2012-08-10'
});

const ENV = process.env.ENV;
const BACKUPS_TO_KEEP = process.env.BACKUPS_TO_KEEP;

exports.handler = async (event, context, callback) => {
    (await getTables(ENV)).forEach((tableName) => {
        dynamodb.listBackups({
            TableName: tableName
        }, (err, data) => {
            if (err) {
                console.log(err);
            } else {
                let backups = _.sortBy(data.BackupSummaries, 'BackupCreationDateTime');

                // Remove the backups we DON'T want to delete.
                let backupsToRemove = BACKUPS_TO_KEEP - 1;
                backups.splice(-backupsToRemove, backupsToRemove);

                // Delete remaining backups
                backups.forEach((backup) => {
                    dynamodb.deleteBackup({
                        BackupArn: backup.BackupArn
                    }, (err, data) => {
                        if (err) {
                            console.log(err);
                        } else {
                            console.log(data);
                        }
                    });
                });

                dynamodb.createBackup({
                    TableName: tableName,
                    BackupName: tableName + '_' + moment().format('YYYYMMDD_HHmmss')
                }, (err, data) => {
                    if (err) {
                        console.log(err);
                    } else {
                        console.log(data);
                    }
                });
            }
        });
    });
    callback(null, "Execution Successful");
};


var getTables = async (env) => {
    try {
        var tempTableList = (await dynamodb.listTables({}).promise()).TableNames;
        return tempTableList.filter(item => String(item).startsWith(env));
    } catch (error) {
        return {
            statusCode: 400,
            error: `Could not post: ${error.stack}`
        };
    }
};
