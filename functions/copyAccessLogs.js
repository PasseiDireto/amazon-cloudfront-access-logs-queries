// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0
const aws = require("aws-sdk");
const s3 = new aws.S3({ apiVersion: "2006-03-01" });

const sourceBucket = process.env.SOURCE_BUCKET;
const sourcePrefix = process.env.SOURCE_PREFIX;
const sourceCloudFrontDistribution = process.env.SOURCE_CLOUDFRONT_DISTRIBUTION;
const targetBucket = process.env.TARGET_BUCKET;
// prefix to copy partitioned data to w/o leading but w/ trailing slash
const targetKeyPrefix = process.env.TARGET_KEY_PREFIX;

exports.handler = async (event, context, callback) => {
  try {
    const date = new Date(event.time);
    date.setHours(date.getHours() - 1);

    const year = date.getUTCFullYear();
    const month = (date.getUTCMonth() + 1).toString().padStart(2, '0');
    const day = date.getUTCDate().toString().padStart(2, '0');
    const hour = date.getUTCHours().toString().padStart(2, '0');

    const listPrefix = `${sourcePrefix}${sourceCloudFrontDistribution}.${year}-${month}-${day}-${hour}`;

    console.log(`Copying ${listPrefix} from bucket ${sourceBucket}`);

    const listParams = {
      Bucket: sourceBucket,
      Prefix: listPrefix,
    };
    const listResult = await s3.listObjects(listParams).promise();

    console.log(`${listResult.Contents.length} logs found`);

    const copies = listResult.Contents.map(async (content) => {
      const [, filename] = content.Key.split(sourcePrefix);
      const targetKey = `${targetKeyPrefix}year=${year}/month=${month}/day=${day}/hour=${hour}/${filename}`;
      const copyParams = {
        CopySource: sourceBucket + "/" + content.Key,
        Bucket: targetBucket,
        Key: targetKey,
      };
      await s3.copyObject(copyParams).promise();
      console.log(`Copied ${filename}.`);
    });

    await Promise.all(copies);
  } catch (error) {
    callback(error);
  }
};
