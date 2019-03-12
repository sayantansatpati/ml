import pandas as pd
import numpy as np


class FeatureStore(object):

    def __init__(self):
        data = self._load_historical_data('historical_data.csv')

        # Create aggregate features from historical data
        # Average delivery time secs for market/store
        self.market_map = data[['market_id', 'delivery_time_secs']].groupby(['market_id']).mean().to_dict()
        self.market_map = self.market_map['delivery_time_secs']
        self.store_map = data[['store_id', 'delivery_time_secs']].groupby(['store_id']).mean().to_dict()
        self.store_map = self.store_map['delivery_time_secs']
        print('[FEATURE_STORE] market_map {0}'.format(len(self.market_map)))
        print('[FEATURE_STORE] store_map {0}'.format(len(self.store_map)))

        # Average delivery time secs per Day of Week for market/store
        self.market_dow_map = data[['market_id', 'day', 'delivery_time_secs']].groupby(['market_id', 'day']).mean().to_dict()
        self.market_dow_map = self.market_dow_map['delivery_time_secs']
        self.store_dow_map = data[['store_id', 'day', 'delivery_time_secs']].groupby(['store_id', 'day']).mean().to_dict()
        self.store_dow_map = self.store_dow_map['delivery_time_secs']
        print('[FEATURE_STORE] market_dow_map {0}'.format(len(self.market_dow_map)))
        print('[FEATURE_STORE] store_dow_map {0}'.format(len(self.store_dow_map)))

        # Average delivery time sec per Hour of day for market/store
        self.market_hour_map = data[['market_id', 'hour', 'delivery_time_secs']].groupby(
            ['market_id', 'hour']).mean().to_dict()
        self.market_hour_map = self.market_hour_map['delivery_time_secs']
        self.store_hour_map = data[['store_id', 'hour', 'delivery_time_secs']].groupby(['store_id', 'hour']).mean().to_dict()
        self.store_hour_map = self.store_hour_map['delivery_time_secs']
        print('[FEATURE_STORE] market_hour_map {0}'.format(len(self.market_hour_map)))
        print('[FEATURE_STORE] store_hour_map {0}'.format(len(self.store_hour_map)))

        # Average delivery time secs per Day of Week, Hour of Day for market/store
        self.market_dow_hour_map = data[['market_id', 'day', 'hour', 'delivery_time_secs']] \
            .groupby(['market_id', 'day', 'hour']).mean().to_dict()
        self.market_dow_hour_map = self.market_dow_hour_map['delivery_time_secs']
        self.store_dow_hour_map = data[['store_id', 'day', 'hour', 'delivery_time_secs']] \
            .groupby(['store_id', 'day', 'hour']).mean().to_dict()
        self.store_dow_hour_map = self.store_dow_hour_map['delivery_time_secs']
        print('[FEATURE_STORE] market_dow_hour_map {0}'.format(len(self.market_dow_hour_map)))
        print('[FEATURE_STORE] store_dow_hour_map {0}'.format(len(self.store_dow_hour_map)))

        self.market_estimated_store_to_consumer_driving_duration_map = \
            data[['market_id', 'estimated_store_to_consumer_driving_duration']] \
                .groupby('market_id').mean().to_dict()
        self.market_estimated_store_to_consumer_driving_duration_map = \
            self.market_estimated_store_to_consumer_driving_duration_map['estimated_store_to_consumer_driving_duration']
        print('[FEATURE_STORE] market_estimated_store_to_consumer_driving_duration_map {0}' \
              .format(len(self.market_estimated_store_to_consumer_driving_duration_map)))

        self.store_estimated_store_to_consumer_driving_duration_map = \
            data[['store_id', 'estimated_store_to_consumer_driving_duration']] \
                .groupby('store_id').mean().to_dict()
        self.store_estimated_store_to_consumer_driving_duration_map = \
            self.store_estimated_store_to_consumer_driving_duration_map['estimated_store_to_consumer_driving_duration']
        print('[FEATURE_STORE] store_estimated_store_to_consumer_driving_duration_map {0}' \
              .format(len(self.store_estimated_store_to_consumer_driving_duration_map)))

    def _load_historical_data(self, historical_data_file):
        print('[FEATURE_STORE] Loading Historical Data {0}'.format(historical_data_file))
        data = pd.read_csv(historical_data_file)

        # Handle the date/time and add delivery_time_secs
        data['created_at'] = pd.to_datetime(data['created_at']).dt.tz_localize('utc').dt.tz_convert('US/Pacific')
        data['actual_delivery_time'] = pd.to_datetime(data['actual_delivery_time']).dt.tz_localize('utc').dt.tz_convert(
            'US/Pacific')
        data['delivery_time_secs'] = (data['actual_delivery_time'] - data['created_at']).dt.total_seconds()

        # Create additional date-time features based on created_at
        data['hour'] = data['created_at'].dt.hour
        data['day'] = data['created_at'].dt.dayofweek

        print('[FEATURE_STORE] Historical Data Shape {0}'.format(data.shape))

        return data

    def add_aggregate_features_delivery_time_secs(self, data_test):
        def add_features_per_record(row):
            avg_delivery_time_market = self.market_map.get(row['market_id'], 0)
            avg_delivery_time_store = self.store_map.get(row['store_id'], 0)

            avg_delivery_time_market_dow = self.market_dow_map.get((row['market_id'], row['day']), 0)
            avg_delivery_time_store_dow = self.store_dow_map.get((row['store_id'], row['day']), 0)

            avg_delivery_time_market_hour = self.market_hour_map.get((row['market_id'], row['hour']), 0)
            avg_delivery_time_store_hour = self.store_hour_map.get((row['store_id'], row['hour']), 0)

            avg_delivery_time_market_dow_hour = self.market_dow_hour_map.get((row['market_id'], row['day'], row['hour']), 0)
            avg_delivery_time_store_dow_hour = self.store_dow_hour_map.get((row['store_id'], row['day'], row['hour']), 0)

            return pd.Series((avg_delivery_time_market, avg_delivery_time_store, \
                              avg_delivery_time_market_dow, avg_delivery_time_store_dow, \
                              avg_delivery_time_market_hour, avg_delivery_time_store_hour, \
                              avg_delivery_time_market_dow_hour, avg_delivery_time_store_dow_hour))

        # Added features
        cols = ['avg_delivery_time_market', 'avg_delivery_time_store', \
                'avg_delivery_time_market_dow', 'avg_delivery_time_store_dow',
                'avg_delivery_time_market_hour', 'avg_delivery_time_store_hour',
                'avg_delivery_time_market_dow_hour', 'avg_delivery_time_store_dow_hour'
                ]

        data_test[cols] = data_test[['market_id', 'store_id', 'day', 'hour']] \
            .apply(add_features_per_record, axis=1)

        return data_test

    def add_aggregate_features_estimated_store_to_consumer_driving_duration(self, data_test):
        def add_features_per_record(row):
            ret = row['estimated_store_to_consumer_driving_duration']
            if not ret:
                ret = self.store_estimated_store_to_consumer_driving_duration_map[row['store_id']]
                if not ret:
                    ret = self.market_estimated_store_to_consumer_driving_duration_map[row['market_id']]
            return ret

        data_test['estimated_store_to_consumer_driving_duration'] = \
            data_test[['market_id', 'store_id', 'estimated_store_to_consumer_driving_duration']] \
            .apply(add_features_per_record, axis=1)
        data_test['estimated_store_to_consumer_driving_duration'] = \
            data_test['estimated_store_to_consumer_driving_duration'].fillna(0)
        return data_test