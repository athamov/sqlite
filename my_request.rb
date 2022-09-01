require 'csv'

class MySqliteRequest
  def initialize
  @where = []
  @db_name = ''
  @select = []
  @request = ''
  @insert_data = []
  end

  def from(table_name)
    if table_name.is_a?(String)
    @db_name = table_name
    else raise ArgumentError
    end
    self
  end

  def select(columns)
    @request = :Select
    if columns.is_a?(Array)
    @select = columns
    else 
      @select.push(columns)
    end
    self
  end

  def where(key,value)
    @where.push([key,value])
    self
  end

  def insert(db_name) 
    @request = :Insert
    @db_name = db_name
    self
  end

  def values(columns)
    @insert_data = columns
    self
  end

  def update(db_name)
    @request = :Update
    @db_name = db_name
    self
  end

  def set(columns)
    @insert_data = columns
    self
  end

  def delete
    @request = :Delete
    self
  end

  def _Rewrite(rewrited)
    CSV.open(@db_name, "w", :headers => true) do |csv| 
        csv << rewrited[0].to_hash.keys
        rewrited.each do |row|
            csv << CSV::Row.new(row.to_hash.keys, row.to_hash.values)
        end
    end
end

  def _Select
    db = CSV.parse(File.read(@db_name), headers: true)
    selected_columns = []
    if @where != [] 
      db.each do |column|
        @where.each do |key, value|
          if column[key]==value  
            selected_columns << column
          end
        end
      end
    else
      selected_columns = db
    end
    selected_columns.each do |row|
      if @select != []
        a = row.select {|key, value| @select.include?(key)  }
        puts a[0][1]
      else
        puts row
      end
    end
  end

  def _Update
    db = CSV.parse(File.read(@db_name), headers: true)
    new_db = []
    db.each do |row|
      check = true
      @where.each do |key, value|
        if row[key] != value
          check = false
        end
      end
      # puts row.to_hash,check
      if check == true
        @insert_data.each {|key, value| row[key] = value }
        
      end
      new_db << row.to_hash
    end
    new_db
  end

  def _Insert
    File.open(@db_name, File::RDWR|File::CREAT, 0644) {|f|
      f.flock(File::LOCK_EX)
      value = f.read
      f.rewind
      insert = @insert_data.values.join(',')
      f.write("#{value}#{insert}")
      f.flush
      f.truncate(f.pos)
    }
  end

  def _Delete
    db = CSV.parse(File.read(@db_name), headers: true)
    new_db = []
    db.each do |row|
      check = true
      @where.each do |key, value|
        if row[key] != value
          check = false
        end
      end
      # puts row.to_hash,check
      if check != true
        new_db << row.to_hash
      end
    end
    new_db
  end

  def run
    case @request
    when :Select 
      _Select
    when :Update 
      new_db = _Update
      _Rewrite(new_db)
    when :Insert
      _Insert
    when :Delete
      new_db = _Delete
      _Rewrite(new_db)
    else raise ArgumentError
    end
  end
end

# request = MySqliteRequest.new
# request = request.from('nba_players.csv')
# request = request.select('Player')
# request = request.where('birth_state', 'Indiana')
# request.run