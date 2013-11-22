define(['angular' ], function (angular) {
	'use strict';

	/* Controllers */
	return angular.module('jumbodb.monitoringControllers', [])
		// Sample controller where service is being used
		.controller('serverMonitoringController', ['$scope', '$http', function ($scope, $http) {
			$http.get('jumbodb/rest/status').success(function(data) {
				$scope.status = data;
			});
		}])
		.controller('queryMonitoringController', ['$scope', '$http', function ($scope, $http) {
			$scope.test = "test query..";
		}])
		.controller('importMonitoringController', ['$scope', '$http', function ($scope, $http) {
			$scope.test = "test import..";
		}])
});

