define([ 'angular', 'app', 'controllers' ], function(angular, app, Controllers) {
	'use strict';

	return app.config([ '$routeProvider', function($routeProvider) {

		$routeProvider.
			when('/overview', {templateUrl: 'partials/overview.html', controller: Controllers.OverviewCtrl}).
			when('/collections', {templateUrl: 'partials/collections.html', controller: Controllers.CollectionsListCtrl}).
			when('/deliveries', {templateUrl: 'partials/deliveries.html', controller: Controllers.DeliveriesListCtrl}).
			when('/browse', {templateUrl: 'partials/browse.html', controller: Controllers.BrowseCtrl}).
			when('/replication', {templateUrl: 'partials/replication.html', controller: Controllers.ReplicationCtrl}).
			when('/serverMonitoring', {templateUrl: 'partials/serverMonitoring.html', controller: Controllers.ServerMonitoringCtrl}).
			when('/queryMonitoring', {templateUrl: 'partials/queryMonitoring.html', controller: Controllers.QueryMonitoringCtrl}).
			when('/importMonitoring', {templateUrl: 'partials/importMonitoring.html', controller: Controllers.ImportMonitoringCtrl}).
			when('/help', {templateUrl: 'partials/help.html', controller: Controllers.HelpCtrl}).
			otherwise({redirectTo: '/overview'});
	} ]);

});

