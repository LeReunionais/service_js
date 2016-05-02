'use strict';
const TIMEOUT = 3000
		, MAX_RETRIES = 3;

var bunyan = require('bunyan')
	, log = bunyan.createLogger({name: "service_js"})
;

var zmq = require('zmq')
	, jsonrpc = require('jsonrpc-lite')
	, uuid = require('node-uuid')
;

var serviceMap = {};

module.exports = {
	whereis: (service, registryHost) => {
		if (serviceMap[service] !== undefined) {
			log.info("Get service %s from cache", service);
			return Promise.resolve(serviceMap[service]);
		} else {
			log.info("Trying to retrieve service %s", service);
			return whereis(service, registryHost, 1)
			.then( (serviceObj) => {
				log.info({service:serviceObj}, "Found %s", service);
				serviceMap[service] = serviceObj;
				return serviceObj;
			});
		}
	},
	invalidate: (service, registryHost) => {
		log.info("Invalidate service %s", service)
		invalidate(service, registryHost);
		serviceMap[service] = undefined;
	}
}

function invalidate(service_name, registryHost) {
	log.info(`Trying to invalidate ${service_name} on ${registryHost}`);
	var pushSocket = zmq.socket('push');
	const endpoint = `tcp://${registryHost}:3001`;
	pushSocket.connect(endpoint);
	log.info("Connected to %s", endpoint);
	const invalidate = {
		name: service_name
	}
	const invalidateMsg = jsonrpc.request(uuid.v4(), "invalidate", invalidate);
	log.info({request:invalidateMsg}, "sent request");
	pushSocket.send(JSON.stringify(invalidateMsg));
}

function whereis(service, registryHost, attempt) {
	log.info(`#${attempt} Attempt to retrieve service information`);
	return new Promise ( (resolve, reject) => {
		var pushSocket = zmq.socket('req')
		const endpoint = `tcp://${registryHost}:3002`;
		pushSocket.connect(endpoint);
		log.info("Connected to %s", endpoint);
		const whereisMsg = jsonrpc.request(uuid.v4(), "find", service);
		log.info({request:whereisMsg}, "sent request");
		pushSocket.send(JSON.stringify(whereisMsg));

		var timeout = setTimeout( () => {
			pushSocket.close();
			const err = "Timeout. No response after: " + TIMEOUT + "ms"
			reject(err);
		}, TIMEOUT);

		pushSocket.on("message", (msg) => {
			pushSocket.close();
			clearTimeout(timeout);
			log.info({request:msg.toString()}, "reply");
			const serviceObj = JSON.parse(msg.toString());
			resolve(serviceObj.result);
		});

		pushSocket.on("error", (err) => {
			pushSocket.close();
			clearTimeout(timeout);
			log.log(err);
			reject(err);
		});
	})
	.catch( (err) => {
		log.error(err);
		if (attempt === MAX_RETRIES) {
			const message = `Not able to contact ${registryHost} after ${MAX_RETRIES} attempts.`;
			log.info(message);
			return Promise.reject(message);
		} else {
			log.info("Retry");
			return whereis(service, registryHost, attempt+1);
		}
	})
}
