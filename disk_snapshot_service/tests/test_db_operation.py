import pytest

from disk_snapshot_service.data_access import db_query as db


class TestJournalQuery:

    def test_get_obj(self):
        token = 't1'
        journal_query = db.JournalQuery(token)
        assert journal_query.get_obj().tree_ident == 'ti1'

    def test_get_inst(self):
        token = 't1'
        journal_query = db.JournalQuery(token)
        assert journal_query.get_inst().new_ident == '112112'

    def test_get_obj_dict(self):
        token = 't1'
        journal_query = db.JournalQuery(token)
        assert journal_query.get_obj_dict()['id'] == 1


class TestUnconsumedJournalsQuery:

    def test_query_objs(self):
        tokens = ['t2', 't3']

#
