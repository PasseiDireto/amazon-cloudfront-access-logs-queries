// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
const aws = require('aws-sdk');
const s3 = new aws.S3({ apiVersion: '2006-03-01' });

// prefix to copy partitioned data to w/o leading but w/ trailing slash
const targetKeyPrefix = process.env.TARGET_KEY_PREFIX;

// regex for filenames by Amazon CloudFront access logs. Groups:
// - 1.	year
// - 2.	month
// - 3.	day 
// - 4.	hour
const datePattern = '[^\\d](\\d{4})-(\\d{2})-(\\d{2})-(\\d{2})[^\\d]';
const filenamePattern = '[^/]+$';

exports.handler = async (event, context, callback) => {
  const copies = event.Records.map(record => {
    const bucket = record.s3.bucket.name;
    const sourceKey = record.s3.object.key;

    const sourceRegex = new RegExp(datePattern, 'g');
    const match = sourceRegex.exec(sourceKey);
    if (match == null) {
      console.log(`Object key ${sourceKey} does not look like an access log file, so it will not be copied.`);
    } else {
      const [, year, month, day, hour] = match;

      const filenameRegex = new RegExp(filenamePattern, 'g');
      const filename = filenameRegex.exec(sourceKey)[0];

      const targetKey = `${targetKeyPrefix}year=${year}/month=${month}/day=${day}/hour=${hour}/${filename}`;
      console.log(`Copying ${sourceKey} to ${targetKey}.`);

      const copyParams = {
        CopySource: bucket + '/' + sourceKey,
        Bucket: bucket,
        Key: targetKey
      };
      const copy = s3.copyObject(copyParams).promise();

      return copy.then(function () {
        console.log(`Copied ${sourceKey}.`);
      }, function (reason) {
        var error = new Error(`Error while copying ${sourceKey}: ${reason}`);
        callback(error);
      });

    }
  });
  await Promise.all(copies);
};
