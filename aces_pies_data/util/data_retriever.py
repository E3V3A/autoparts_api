# class DataRetriever(object):
#     """
#     This is a helper class for bulk inserts
#     It will automatically retrieve the records desired based
#     off a query set and will create any missing records.
#     It uses a key for record lookup to increase performance dramatically
#     """
#
#     def __init__(self, model_cls, query_set, key_parts):
#         self.model_cls = model_cls
#         self.query_set = query_set
#         self.key_parts = key_parts
#         self.record_lookup = None
#
#     def set_record_lookup(self):
#         if not self.record_lookup:
#             self.record_lookup = dict()
#             for record in self.query_set:
#                 self.record_lookup[self.get_record_key(record, self.key_parts)] = record
#
#     @staticmethod
#     def get_record_key(data_item, key_parts, is_django_model=True):
#         record_key = ''
#         if not is_django_model:
#             for key_part in key_parts:
#                 key_value = '' if data_item[key_part] is None else str(data_item[key_part])
#                 record_key += key_value
#         else:
#             for key_part in key_parts:
#                 next_attr = data_item
#                 key_tokens = key_part.split('__')
#                 for idx, key_token in enumerate(key_tokens):
#                     if idx == 0 and isinstance(data_item, dict):
#                         next_attr = next_attr[key_token]
#                     else:
#                         if next_attr is not None:
#                             next_attr = getattr(next_attr, key_token)
#                     if next_attr is None:
#                         next_attr = ''
#                         break
#                 record_key += str(next_attr)
#         return record_key
#
#     def get_instance(self, record_key):
#         self.set_record_lookup()
#         if record_key in self.record_lookup:
#             return self.record_lookup[record_key]
#         return None
#
#     def get_records(self):
#         self.set_record_lookup()
#         return self.record_lookup
#
#     def bulk_get_or_create(self, data_list):
#         """
#         data_list is the data to get or create
#         We generate the query and set all the record keys based on passed in queryset
#         Then we loop over each item in the data_list, which has the keys already! No need to generate them.
#         Args:
#             data_list:
#
#         Returns:
#
#         """
#         items_to_create = dict()
#         for data_item in data_list:
#             record_key = self.get_record_key(data_item, self.key_parts)
#             if record_key not in items_to_create:
#                 record = self.get_instance(record_key)
#                 if not record:
#                     items_to_create[record_key] = self.model_cls(**data_item)
#         if items_to_create:
#             """
#             TODO.  I think we can optimize this.  Switch to values, get the primary id
#             Query set is just select the model with that ID.  Return the model object without running the full queryset again.  Should be a lot faster.
#             """
#
#             self.model_cls.objects.bulk_create(items_to_create.values())
#             self.query_set = self.query_set.all()
#             self.record_lookup = None
#             self.set_record_lookup()
#         return self.record_lookup


class DataRetriever(object):
    """
    This is a helper class for bulk inserts
    It will automatically retrieve the records desired based
    off a query set and will create any missing records.
    It uses a key for record lookup to increase performance dramatically
    """

    def __init__(self, model_cls, query_set, key_parts):
        self.model_cls = model_cls
        self.key_parts = key_parts
        self.query_set = query_set.values(*(["id"] + list(self.key_parts)))
        self.record_lookup = None

    def set_record_lookup(self, force=False):
        if force:
            self.record_lookup = None
            self.query_set = self.query_set.all()
        if not self.record_lookup:
            self.record_lookup = dict()
            for record in self.query_set:
                self.record_lookup[self.get_record_key(record, self.key_parts)] = record['id']

    @staticmethod
    def get_record_key(data_item, key_parts):
        record_key = ''
        for key_part in key_parts:
            key_value = '' if data_item[key_part] is None else str(data_item[key_part])
            record_key += key_value
        return record_key

    def get_instance(self, record_key):
        self.set_record_lookup()
        if record_key in self.record_lookup:
            return self.record_lookup[record_key]
        return None

    def get_records(self):
        self.set_record_lookup()
        return self.record_lookup

    def bulk_get_or_create(self, data_list):
        """
        data_list is the data to get or create
        We generate the query and set all the record keys based on passed in queryset
        Then we loop over each item in the data_list, which has the keys already! No need to generate them.  Should save a lot of time
        Use values instead of the whole object, much faster
        Args:
            data_list:

        Returns:

        """
        items_to_create = dict()
        for record_key, record_config in data_list.items():
            if record_key not in items_to_create:
                record = self.get_instance(record_key)
                if not record:
                    items_to_create[record_key] = self.model_cls(**record_config)
        if items_to_create:
            """
            TODO.  I think we can optimize this.  Switch to values, get the primary id
            Query set is just select the model with that ID.  Return the model object without running the full queryset again.  Should be a lot faster.
            """

            self.model_cls.objects.bulk_create(items_to_create.values())
            self.set_record_lookup(True)
        return self.record_lookup
