import AWS from 'aws-sdk';
import http from 'http';

const s3 = new AWS.S3();
const secrets = require(`../secrets.${process.env.STAGE}.json`);

export default async (event) => {
  const srcBucket = event.Records[0].s3.bucket.name;
  const srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
  let fileBody;

  //download item from bucket
  try {
    const params = {
      Bucket: srcBucket,
      Key: srcKey
    };
    const origFile = await s3.getObject(params).promise();

    if (origFile.Body) {
      const json = JSON.stringify(origFile.Body);
      const bufferOriginal = Buffer.from(JSON.parse(json).data);
      fileBody = bufferOriginal.toString('utf8');
    }
  } catch (err) {
    console.error(err.message);
    throw new Error(`[500] Internal Server Error ${JSON.stringify(err.message)}`);
  }

  try {
    await writeToKafka(fileBody);
    return 'success';
  } catch (err) {
    console.error(err.message);
    throw new Error(`[500] Internal Server Error ${JSON.stringify(err.message)}`);
  }
};

function writeToKafka(event) {
  return new Promise((resolve, reject) => {
    const options = {
      headers: {
        'X-SDC-APPLICATION-ID': 'abc',
        'Content-Type': 'application/x-www-form-urlencoded'
      },
      host: secrets.STREAMSET_URL,
      port: 8103,
      method: 'POST'
    };

    const req = http.request(options, (res) => {
      const response = {
        statusCode: 200,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Credentials': true
        },
        body: JSON.stringify('Success')
      };
      resolve(response);
    });

    req.on('error', (err) => {
      console.error(`upload-error: ${err}`);
      console.error(`upload-error: ${err.stack}`);
      reject(err.message);
    });

    // send the request
    if (event.hasOwnProperty('body')) req.write(event.body);
    else req.write(JSON.stringify(event));
    req.end();
  });
}
