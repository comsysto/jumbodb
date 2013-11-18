angular.module('jumbodb', []).
    config(['$routeProvider', function ($routeProvider) {
        $routeProvider.
            when('/overview', {templateUrl: 'partials/overview.html', controller: OverviewCtrl}).
            when('/collections', {templateUrl: 'partials/collections.html', controller: CollectionsListCtrl}).
            when('/deliveries', {templateUrl: 'partials/deliveries.html', controller: DeliveriesListCtrl}).
            when('/browse', {templateUrl: 'partials/browse.html', controller: BrowseCtrl}).
            when('/replication', {templateUrl: 'partials/replication.html', controller: ReplicationCtrl}).
			when('/serverMonitoring', {templateUrl: 'partials/serverMonitoring.html', controller: ServerMonitoringCtrl}).
			when('/queryMonitoring', {templateUrl: 'partials/queryMonitoring.html', controller: QueryMonitoringCtrl}).
			when('/importMonitoring', {templateUrl: 'partials/importMonitoring.html', controller: ImportMonitoringCtrl}).
            when('/help', {templateUrl: 'partials/help.html', controller: HelpCtrl}).
            otherwise({redirectTo: '/overview'});
    }]);