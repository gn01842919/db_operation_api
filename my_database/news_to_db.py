import random
from datetime import datetime
# PyPI
import pytz
# Local modules
from database import MyPostgreSqlDB


class NewsDataToDB(object):
    def __init__(self, conn, table_prefix=""):
        self.conn = conn
        self.table_prefix = table_prefix

    def _add_prefix(self, table_name):
        return self.table_prefix + table_name

    def store_a_scraping_rule_to_db(self, rule):

        if not isinstance(rule, ScrapingRule):
            raise RuntimeError(
                "Parameter 'rule' (%s) should be an instance of ScrapingRule"
                % repr(rule)
            )

        self._insert_into_table("scrapingrule", name=rule.name, active=True)

        for tag in rule.tags:
            self._insert_into_table("newscategory", name=tag)

        for keyword in rule.included_keywords:
            self._insert_into_table("newskeyword", name=keyword, to_include=True)

        for keyword in rule.excluded_keywords:
            self._insert_into_table("newskeyword", name=keyword, to_include=False)

    def store_a_rss_news_entry_to_db(self, news):

        if not isinstance(news, NewsRSSEntry):
            raise RuntimeError(
                "Parameter 'news' (%s) should be an instance of NewsRSSEntry"
                % repr(news)
            )
        curr_time = datetime.now(pytz.utc)
        self._insert_into_table(
            "newsdata",
            title=news.title,
            url=news.link,
            content=news.description,
            time=news.published_time,
            creation_time=curr_time,
            last_modified_time=curr_time
        )

    def _get_object_id(self, table_name, **kwargs):
        table_name = self._add_prefix(table_name)
        rows = self.conn.get_field_by_conditions(table_name, "id", kwargs)
        if rows:
            return rows[0][0]
        else:
            raise RuntimeError(
                "Can not get entry id from table '{}' with condition {}."
                .format(table_name, kwargs)
            )

    def _insert_into_table(self, table_name, **kwargs):
        table_name = self._add_prefix(table_name)
        self.conn.insert_values_into_table(table_name, kwargs)


# For test
class ScrapingRule(object):
    def __init__(self, name):
        rand_num = str(random.randint(1, 100000))
        self.name = name + rand_num
        self.included_keywords = {'inc_kw' + rand_num, 'inc_kw' + rand_num}
        self.excluded_keywords = {'exc_kw' + rand_num, 'exc_kw' + rand_num}
        self.tags = {'AA' + rand_num, 'BBB' + rand_num, 'CC' + rand_num}

    def __str__(self):
        return "<ScrapingRule '%s'>" % self.name


class NewsRSSEntry(object):
    def __init__(
        self, title, desc, link, published_time, source, category=None, tags=None
    ):
        self.title = title
        self.description = desc
        self.link = link
        self.published_time = published_time
        self.source = source
        self.rule_score_map = {}
        self.tags = tags.copy() if tags else set()  # .copy() -> shallow copy

        if category:
            self.tags.add(category)

    def set_rules(self, rules):
        for rule in rules:
            score = self._compute_score_by_rule(rule)
            self.rule_score_map[rule] = score

    def _compute_score_by_rule(self, rule):
        return random.randint(1, 30)

    def __repr__(self):
        return (
            "  #-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#\n"
            "  -- <NewsRSSEntry object at {0}> --\n"
            "  [Title]       : {news_obj.title}\n"
            "  [Description] : {news_obj.description}\n"
            "  [Link]        : {news_obj.link}\n"
            "  [Published]   : {news_obj.published_time}\n"
            "  [Source]      : {news_obj.source}\n"
            "  [Tags]        : {news_obj.tags}\n"
            "  [Rules]       : {rules}\n"
            "  #-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#\n"
            .format(
                hex(id(self)),
                news_obj=self,
                rules={str(rule): score for rule, score in self.rule_score_map.items()}
            )
        )

    def __str__(self):
        return "<NewsRSSEntry '%s'>" % self.title


if __name__ == "__main__":

    rand_num = str(random.randint(1, 100000))

    rule = ScrapingRule("Rule #" + rand_num)
    news = NewsRSSEntry(
        title="News #" + rand_num,
        desc="Description of News #" + rand_num,
        link="https://abc.cpm/" + rand_num,
        published_time=datetime.now(),
        source='Fake source',
        category='TESTING'
    )
    news.set_rules((rule,))

    with MyPostgreSqlDB(database="my_focus_news") as conn:
        d = NewsDataToDB(conn, table_prefix="shownews_")
        print(d._get_object_id("newsdata", url='https://abc.cpm/56655'))
        d.store_a_scraping_rule_to_db(rule)
        d.store_a_rss_news_entry_to_db(news)
