class IssueObj:
    def __init__(self, data):
        self.number = data.get('number')
        self.id = data.get('id')
        self.name = data.get('name')
        self.store_date = data.get('store_date')
        self.cover_date = data.get('cover_date')
        self.image = data.get('image')


class SeriesObj:
    def __init__(self, data):
        self.name = data.get('name')
        self.volume = data.get('volume')
        self.id = data.get('id')
