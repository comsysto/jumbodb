require.config({
	//baseUrl : "js",
	paths: {
		angular: "../jumbodb/resources/angularjs/angular",
		angularUiBootstrap: 'libs/ui-bootstrap-0.7.0',
		angularUiBootstrapTpls: 'libs/ui-bootstrap-tpls-0.7.0',
		dimple: 'libs/dimple.v1.1.2',
		d3js: "../jumbodb/resources/d3js/d3",
		underscore:'../jumbodb/resources/underscorejs/underscore'
	},
	shim: {
		'angular' : {exports : 'angular'},
		'angularUiBootstrap': {exports : 'angularUi',
								deps: ['angular']},
		'angularUiBootstrapTpls': {deps: ['angular', 'angularUiBootstrap']},
		'dimple' : {deps: ['d3js']},
		'underscore':{exports:'_'}
	},
	priority: [
		"angular",
		"angularUiBootstrap",
		"angularUiBootstrapTpls"
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

