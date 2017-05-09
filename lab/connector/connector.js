Kafka_JS = Class.create();

var SUCCESS = Packages.com.service_now.mid.probe.tpcon.OperationStatusType.SUCCESS;
var FAILURE = Packages.com.service_now.mid.probe.tpcon.OperationStatusType.FAILURE;
var Event   = Packages.com.snc.commons.eventmgmt.Event;
var SNEventSenderProvider = Packages.com.service_now.mid.probe.event.SNEventSenderProvider;
var HTTPRequest = Packages.com.glide.communications.HTTPRequest;
var MetricFactory = Packages.com.service_now.metric.MetricFactory;
var RawMetric = Packages.com.service_now.metric.model.RawMetric;

var MAX_EVENTS_TO_FETCH = 3000;
var errorMessage = "";
var consumer_id;
var kafka_instance;

var topics;
var max_retry_count = 2;
var current_retry = 0;

Kafka_JS.prototype = Object.extendsObject(AProbe, {
	testConnection : function() {
		ms.log("Connector testing connection");
		kafka_instance = this.probe.getAdditionalParameter("kafka_instance");
		var retVal = {};
			
		if(kafka_instance) {
			try {
				var response = this.getResponse(this.topicQuery());
				if (response){
					retVal['status']  = SUCCESS.toString();
				}
				else{
					this.addError(response.getErrorMessage());
					retVal['status']  = FAILURE.toString();
				}
			} catch (e) {
				this.addError(e.toString());
				retVal['status'] = FAILURE.toString();
			}
		} else {
			this.addError("Missing parameter: kafka_instance");
			retVal['status']  = FAILURE.toString();
		}
		ms.log("Connector Connector testConnection " + retVal['status'] );
		if (retVal['status'] === FAILURE.toString())
			retVal['error_message'] = errorMessage;
		return retVal;
	},
		
	topicQuery: function() {
		return kafka_instance + "/topics";
	},
		
	execute: function() {
		ms.log("Connector Connector: execute connection ...");
		var retVal = {};
			retVal['status'] = SUCCESS.toString();
		return retVal;
	},
			
	retrieveKpi: function() {
		ms.log("Kafka_Rest_JS Connector: retrieveKpi ...");
				
		kafka_instance = this.probe.getAdditionalParameter("kafka_instance");
		consumer_id = this.probe.getAdditionalParameter("consumer_id");
			
		var retVal = {};
		var metricHandler = MetricFactory.getMetricHandler();
					
		try {
			for(var j =0; j < 20;j++) {
				var records = this.getMessages();
				for (var i =0; i < records.length; i++) {
					var rawMetric = this.transformMessage(records[i].value);
					metricHandler.handleMetric(rawMetric);
				}
			}
			retVal['status']  = SUCCESS.toString();
		} catch(e) {
			this.addError(e);
			retVal['status']  = FAILURE.toString();
		}
					
		if (retVal['status'] === FAILURE.toString())
			retVal['error_message'] = errorMessage;
		return retVal;
	},
				
	transformMessage : function(rawMetric) {
		var ciIdentifier = {};
		var resource = "";
		for(var i =0; i< rawMetric.dimensions.length; i++) {
			ciIdentifier[rawMetric.dimensions[i].name] = rawMetric.dimensions[i].value;
			if(rawMetric.dimensions[i].name == "MonitoringObjectName")
				ciIdentifier.name = rawMetric.dimensions[i].value;
			if(rawMetric.dimensions[i].name == "MonitoringObjectType")
				resource = rawMetric.dimensions[i].value;
		}
			
		//var transformedMetric = new RawMetric(metricName, null, rawMetric.resource, rawMetric.node, null, rawMetric.value, rawMetric.timestamp, ciIdentifier, "Kafka" /*source*/, "Kafka-connector" /*source instance*/, null);
			
		var transformedMetric = new RawMetric(rawMetric.metricname, null, resource, null, null, rawMetric.value, rawMetric.timestamp, ciIdentifier, "K17Demo", "Kafka-connector", null);
		return transformedMetric;
	},
					
	getConsumerInstanceURL : function () {
		return this.getCreateConsumerURL() + "/instances/" + consumer_id;
	},
					
	getRecordsURL : function() {
		return this.getConsumerInstanceURL() + "/records";
	},
	
	getMessages: function() {
		var records = [];
						
		if(current_retry == max_retry_count)
			return records;
						
		records = this.getResponse(this.getRecordsURL());
			
		if(records && records['error_code'] !== undefined) {
			if(records['error_code'] === 40403) {
				// Consumer instance not found
				// create consumer instance
				this.createConsumer();
				// subscribe to topic
				this.subscribeTopic();
				// try to get the records again.
				current_retry++;
				return this.getMessages();
			}
		} else {
			current_retry = 0;
		}
						
		return records;
	},
					
	getResponse: function(query) {
		//return parsed response according to the query type (such as REST or DB);
		return this.getResponseJSON(query);
	},
					
	//helper method - creates HTTP request and returns the response as JSON string
	getResponseJSON: function(query) {
		var request = this.createRequest(query);
		request.addHeader('Accept','application/vnd.kafka.json.v2+json');
		var response = request.get();
		if (response == null)
			this.addError(request.getErrorMessage());
			return this.parseToJSON(response);
	},
					
	createRequest: function(query) {
		var request = new HTTPRequest(query);
		return request;
	},
				
	//helper method - returns the response after parsing it to JSON
	parseToJSON : function (response) {
		var parser = new JSONParser();
		var resultJson =  parser.parse(response.getBody());
		ms.log("Connector: Found " + resultJson.length + " records");
		return resultJson;
	},
					
	getCreateConsumerURL : function() {
		return kafka_instance + "/consumers/" + consumer_id;
	},
				
	createConsumer: function() {
		var content = {};
		content["name"] = consumer_id;
		content["format"] = "json";
		content["auto.offset.reset"] = "earliest";
		var request = this.createRequest(this.getCreateConsumerURL());
		request.addHeader('Content-Type','application/vnd.kafka.json.v2+json');
		var response = request.post(JSON.stringify(content));
	},
						
	getSubscriptionURL : function() {
		return this.getConsumerInstanceURL() + "/subscription";
	},
						
	subscribeTopic: function() {
		var topics = this.probe.getAdditionalParameter("topics").split(",");
		var content = {};
		content["topics"] = topics;
		var request = this.createRequest(this.getSubscriptionURL());
		request.addHeader('Content-Type','application/vnd.kafka.json.v2+json');
		var response = request.post(JSON.stringify(content));
		if(response == null || !response.getBody())
			this.addError("Failed to subscribe to topic.");
	},
							
	addError : function(message){
		if (errorMessage === "")
			errorMessage = message;
		else
			errorMessage += "\n" + message;
			ms.log(message);
	},
	
	type: "Kafka_JS"
});