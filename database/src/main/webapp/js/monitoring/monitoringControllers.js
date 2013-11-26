define(['angular', 'dimple', 'underscore' ], function (angular) {
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
			this.drawNumberOfQueriesChart = function (){
				var svg = dimple.newSvg("div#firstChart", $("div#firstChart").width(), $("div#firstChart").height());
				var data = [
					{ "Word":"Hello", "Awesomeness":2000 },
					{ "Word":"World", "Awesomeness":3000 }
				];
				var chart = new dimple.chart(svg, data);
				chart.addCategoryAxis("x", "Word");
				chart.addMeasureAxis("y", "Awesomeness");
				chart.addSeries(null, dimple.plot.bar);
				chart.draw();
			};

		}])
		.controller('importMonitoringController', ['$scope', '$http', function ($scope, $http) {
			$scope.test = "test import..";
		}])
});

