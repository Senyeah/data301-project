"""
countries.py: Returns a list of ISO 3166-1 alpha-3 country codes as the basis for analysis, this is
the format used by GDELT
"""
from abc import ABC, abstractmethod
from pyquery import PyQuery
from collections import namedtuple
from pprint import pprint

AnalysableCountry = namedtuple('AnalysableCountry', 'name code classification')

# Downloads raw HTML for a page and initialises an HTML parser to scrape content
class IndexablePage(ABC):
  # Page to download
  source = None

  def __init__(self):
    if self.source is None:
      raise Exception('empty source!')
    self.query = PyQuery(url=self.source)

  @abstractmethod
  def countries(): pass

# Represents the implementation for Wikipedia's HDI index page
class HDIIndexes(IndexablePage):
  # Represents an entry in the table
  Value = namedtuple('HDIValue', 'country score')

  # Page URL
  source = 'https://en.wikipedia.org/wiki/List_of_countries_by_Human_Development_Index'

  def asfloat(self, string):
    try:
      return float(string)
    except:
      return string

  def process_row(self, index, element):
    cols = PyQuery(element).children('td').filter(
      lambda i, _: i in [2, 3] # Col 2 is name, 3 HDI score
    ).map(
      lambda _, e: self.asfloat(PyQuery(e).text())
    )
    return HDIIndexes.Value(*cols)

  def country_rows_bounded(self, start_idx, end_idx):
    return self.query(
      f'tr:gt({start_idx + 1}):lt({end_idx + 1})'
    ).map(self.process_row)

  def countries(self, **kwargs):
    # The page has a table that comes after an unordered list, entries are in rows
    rows = self.query('ul + table tr')

    # Determine what headings (e.g. Very High, High) that are present
    headings = rows.filter(lambda _, e: PyQuery(e).children('td[colspan="5"]').length > 0)

    # ...then determine the indexes of the headings and add the last item
    indexes = list(rows.map(lambda i, e: i if e in headings else None))
    indexes.append(rows.length)

    # Collection of row indexes which define particular sections
    section_countries = [
      self.country_rows_bounded(start, end) for end, start in zip(indexes[1:], indexes)
    ]

    # Determine what countries to return given from the arguments passed:
    #      top=n: Return the top n countries
    #   bottom=n: Return the bottom n countries
    # ...from each section
    country_filters = {
      'all': lambda idx, n, len: True,
      'top': lambda idx, n, len: idx < n,
      'bottom': lambda idx, n, len: idx > (len - n - 1)
    }

    # By default, return every country
    should_include, value = country_filters['all'], None
    for key, value in kwargs.items():
      if key in country_filters:
        should_include, value = country_filters[key], value

    # Filter only those countries which match the criteria
    filtered = map(
      lambda section: [
        country for index, country in enumerate(section)
        if should_include(index, value, len(section))
      ], section_countries
    )

    return list(filtered)

if __name__ == '__main__':
  test = HDIIndexes()
  pprint(test.countries(top=5)[0])
