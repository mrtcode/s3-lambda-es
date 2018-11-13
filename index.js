/*
 ***** BEGIN LICENSE BLOCK *****
 
 This file is part of the Zotero Data Server.
 
 Copyright © 2018 Center for History and New Media
 George Mason University, Fairfax, Virginia, USA
 http://zotero.org
 
 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU Affero General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.
 
 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU Affero General Public License for more details.
 
 You should have received a copy of the GNU Affero General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 
 ***** END LICENSE BLOCK *****
 */

const AWS = require('aws-sdk');
const elasticsearch = require('elasticsearch');
const config = require('./config');

const SQS = new AWS.SQS({apiVersion: '2012-11-05'});
const Lambda = new AWS.Lambda({apiVersion: '2015-03-31'});

const es = new elasticsearch.Client({
	host: config.es.host
});

const esOld = new elasticsearch.Client({
	host: config.esOld.host
});

const s3 = new AWS.S3(config.s3);

async function esIndex(data) {
	let id = data.libraryID + '/' + data.key;
	
	// Key is not needed
	data.key = undefined;
	
	try {
		await es.index({
			index: config.es.index,
			type: config.es.type,
			id: id,
			version: data.version,
			version_type: 'external_gt',
			routing: data.libraryID,
			body: data
		});
	}
	catch (err) {
		// Ignore 'version_conflict_engine_exception'
		if (err.status !== 409) {
			throw err;
		}
	}
	
	// Old ES index
	await esOld.index({
		index: config.esOld.index,
		type: config.esOld.type,
		id: id,
		routing: data.libraryID,
		body: data
	});
}

async function esDelete(libraryID, key) {
	let id = libraryID + '/' + key;
	
	await es.delete({
		index: config.es.index,
		type: config.es.type,
		id: id,
		routing: libraryID,
	});
	
	// Old ES index
	await esOld.delete({
		index: config.esOld.index,
		type: config.esOld.type,
		id: id,
		routing: libraryID,
	});
}

async function processEvent(event) {
	// Always gets only one event per invocation
	let eventName = event.Records[0].eventName;
	let bucket = event.Records[0].s3.bucket.name;
	let key = event.Records[0].s3.object.key;
	
	if (/^ObjectCreated/.test(eventName)) {
		let data = await s3.getObject({Bucket: bucket, Key: key}).promise();
		let json = JSON.parse(data.Body.toString());
		await esIndex(json);
	}
	else if (/^ObjectRemoved/.test(eventName)) {
		let parts = key.split('/');
		await esDelete(parts[0], parts[1]);
	}
}

exports.s3 = async function (event) {
	await processEvent(event);
};

exports.dlq = async function (event, context) {
	let params;
	try {
		// Process one DLQ message per lambda invocation
		params = {
			QueueUrl: config.sqsUrl,
			MaxNumberOfMessages: 1,
			VisibilityTimeout: 10,
		};
		let data = await SQS.receiveMessage(params).promise();
		
		if (!data || !data.Messages || !data.Messages.length) return;
		
		let message = data.Messages[0];
		
		await processEvent(JSON.parse(message.Body));
		
		params = {
			QueueUrl: config.sqsUrl,
			ReceiptHandle: message.ReceiptHandle,
		};
		await SQS.deleteMessage(params).promise();
	}
	catch (err) {
		console.log(err);
		return;
	}
	
	// Recursively invoke the same lambda function, if the current function
	// invocation successfully processed a message
	params = {
		FunctionName: context.functionName,
		InvocationType: 'Event',
		Payload: JSON.stringify(event),
		Qualifier: context.functionVersion
	};
	
	Lambda.invoke(params);
};
