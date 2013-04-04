function OverviewCtrl($scope, $http) {
    $http.get('/jumbodb/rest/status').success(function(data) {
        $scope.status = data;
    });

}

function CollectionsListCtrl($scope, $http) {
    fetchData();
    $scope.msg = {}

    $scope.activateChunkedVersionInCollection = function(chunkDeliveryKey, version, collection) {
        $http.put('/jumbodb/rest/version/' + chunkDeliveryKey + '/' + version + '/' + collection).success(function(data) {
            fetchData();
            buildMessage(data)
        });
    }

    $scope.deleteChunkedVersionInCollection = function(chunkDeliveryKey, version, collection) {
        $http.delete('/jumbodb/rest/version/' + chunkDeliveryKey + '/' + version + '/' + collection).success(function(data) {
            fetchData();
            buildMessage(data);
        });
    }

    $scope.deleteCompleteCollection = function(collection) {
        $http.delete('/jumbodb/rest/collection/' + collection).success(function(data) {
            fetchData();
            buildMessage(data)
        });
    }

    function buildMessage(data) {
        var msg = {};
        msg.error = (data.type == 'delete')
        msg.success = (data.type == 'activate')
        msg.message = data.message;
        $scope.msg = msg;
    }

    function fetchData() {
        $http.get('/jumbodb/rest/collections').success(function(data) {
            $scope.collections = data;
        });
    }
}

function DeliveriesListCtrl($scope, $http) {
    fetchData();
    $scope.msg = {}

    // CARSTEN reuse ?

    $scope.activateChunkedVersionForAllCollections = function(chunkDeliveryKey, version) {
        $http.put('/jumbodb/rest/version/' + chunkDeliveryKey + '/' + version).success(function(data) {
            fetchData();
            buildMessage(data)
        });
    }

    $scope.activateChunkedVersionInCollection = function(chunkDeliveryKey, version, collection) {
        $http.put('/jumbodb/rest/version/' + chunkDeliveryKey + '/' + version + '/' + collection).success(function(data) {
            fetchData();
            buildMessage(data)
        });
    }

    $scope.deleteChunkedVersionForAllCollections = function(chunkDeliveryKey, version) {
        $http.delete('/jumbodb/rest/version/' + chunkDeliveryKey + '/' + version).success(function(data) {
            fetchData();
            buildMessage(data);
        });
    }

    $scope.deleteChunkedVersionInCollection = function(chunkDeliveryKey, version, collection) {
        $http.delete('/jumbodb/rest/version/' + chunkDeliveryKey + '/' + version + '/' + collection).success(function(data) {
            fetchData();
            buildMessage(data);
        });
    }

    function buildMessage(data) {
        var msg = {};
        msg.error = (data.type == 'delete')
        msg.success = (data.type == 'activate')
        msg.message = data.message;
        $scope.msg = msg;
    }

    function fetchData() {
        $http.get('/jumbodb/rest/deliveries').success(function(data) {
            $scope.deliveries = data;
        });
    }
}