class FilterRunner:
    indices = []
    python_filters = []
    query_filters = []
    
    def starts_with(self, field_name, string):
        if (field_name in self.indices):
            self.query_filters.append((filed_name + ' LIKE ?', '%' + string));
        else:
            self.python_filters.append(('starts_with', string))
            
    def ends_with(self, field_name, string):
        if (field_name in self.indices):
            self.query_filters.append((filed_name + ' LIKE ?', string + '%'));
        else:
            self.python_filters.append(('ends_with', string))
    
    
    def equals(self, field_name, value):
        if (field_name in self.indices):
            self.query_filters.append((filed_name + ' = ?', value));
        else:
            self.python_filters.append(('equals', string))
    
    def between(self, field_name, starting, ending):
        self.larger(field_name, starting)
        self.smaller(field_name, ending)
        
    def larger(self, field_name, value):
        if (field_name in self.indices):
            self.query_filters.append((filed_name + ' > ?', value));
        else:
            self.python_filters.append(('larger', value))

    def smaller(self, field_name, value):
        if (field_name in self.indices):
            self.query_filters.append((filed_name + ' < ?', value));
        else:
            self.python_filters.append(('smaller', value))
            
            
    def build_query_string(self):
        conditions = []
        params = []
        
        for condition, value in self.query_filters:
            conditions.append(condition) 
            params.append(value)
            
        return ' AND '.join(conditions), params
        
    def check_row(self, row):
        '''Checks a row against filters on unindexed data. '''
        pass # TODO apply filters
