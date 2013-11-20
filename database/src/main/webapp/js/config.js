require.config({
	//baseUrl : "js",
	paths: {
		angular: "../jumbodb/resources/angularjs/angular",
		angularUiBootstrap: '../jumbodb/resources/angular-ui-bootstrap/ui-bootstrap',
		angularUiBootstrapTpls: '../jumbodb/resources/angular-ui-bootstrap/ui-bootstrap-tpls'
	},
	shim: {
		'angular' : {exports : 'angular'},
		'angularUiBootstrap': {exports : 'angularUi',
								deps: ['angular']},
		'angularUiBootstrapTpls': {deps: ['angular', 'angularUiBootstrap']}
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

