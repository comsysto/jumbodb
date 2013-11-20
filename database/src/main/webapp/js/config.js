require.config({
	//baseUrl : "js",
	paths: {
		angular: "/jumbodb/jumbodb/resources/angularjs/angular"
		//angularRoute: '/jumbodb/jumbodb/resources/angularjs/angular-route'

	},
	shim: {
		'angular' : {'exports' : 'angular'}
		//'angularRoute': ['angular']
	},
	priority: [
		"angular"
	]
});

require( ['angular','app', 'routes'
], function(angular, app, routes) {
	'use strict';
	var $html = angular.element(document.getElementsByTagName('html')[0]);

	angular.element().ready(function() {
		$html.addClass('ng-app');
		angular.bootstrap($html, [app['name']]);
	});
});

