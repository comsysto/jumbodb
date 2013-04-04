function OverviewCtrl($scope, $http) {
    $http.get('/jumbodb/rest/status').success(function(data) {
        $scope.status = data;
    });

}

function CollectionsListCtrl($scope, $http) {
    $http.get('/jumbodb/rest/collections').success(function(data) {
        $scope.collections = data;
    });
}

function DeliveriesListCtrl($scope, $http) {
    $http.get('/jumbodb/rest/deliveries').success(function(data) {
        $scope.deliveries = data;
    });
}