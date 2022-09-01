require 'readline'
require_relative "my_request"
require 'csv'

# p commands["SELECT"][args]
class MyCli 
  def initialize
    @args = nil
    @main_statements = {
      "SELECT " => Proc.new {_select_type(@args)},
      "INSERT " => Proc.new {_insert_type(@args)}, 
      "UPDATE " => Proc.new {_update_type(@args)}, 
      "DELETE " => Proc.new {_delete_type(@args)},
     }

    #  commands = 
    #  {
    #  "FROM"   => Proc.new {request.from(*args)}, 
    #  "WHERE"  => Proc.new {request.where(col, val)}, 
    #  "VALUES" => Proc.new {request.values(va_set_type(args))}, 
    #  "SET"    => Proc.new {request.set(va_set_type(args))}, 
    #  }
  end
  # @commands = ["SELECT", "FROM", "WHERE", "INSERT", "VALUES", "UPDATE", "SET", "DELETE"]



  def run_buf
   return  Readline.readline('my_sqlite> ', true)
  end

  def delete_semicolon(buf)
    is_end_semicolon = buf.end_with?(";")

    if is_end_semicolon 
      no_semicolon_buf = buf.delete(";")
      no_semicolon_buf
    else
      nil
    end

  end

  def _select_type(args)
    request  = MySqliteRequest.new
    
    from_index = args.index(/[Ff][Rr][Oo][Mm]/)
    where_index = args.index(/[Ww][Hh][Ee][Rr][Ee]/)||0

    select_columns = CSV.parse(args[0..from_index-1].delete(" "))
    db_name = args[from_index+4..where_index-1].strip
    puts select_columns
    request.select(select_columns[0])
    request.from(db_name)

    if where_index
      where_columns = CSV.parse(args[where_index+5..-1].delete(" "))
      where_columns[0].each do |attributes|
        key, value = attributes.split("=")
        request.where(key,value)
      end
    end
    request.run
  end

  def _insert_type(args)
    request  = MySqliteRequest.new
    insert_attributs={}

    into_index = args.index(/[Ii][Nn][Tt][Oo]/)
    values_index = args.index(/[Vv][Aa][Ll][Uu][Ee][Ss]/)

    db_name = args[into_index+4..values_index-1].split(" ")[0]

    key_bracket = (0 ... values_index-1).find_all { |i| args[i,1] == '('|| args[i,1]==')'}
    values_bracket = (values_index+6 ... args.length).find_all { |i| args[i,1] == '('|| args[i,1]==')'}

    keys = CSV.parse(args[key_bracket[0]+1..key_bracket[1]-1])
    values = CSV.parse(args[values_bracket[0]+1..values_bracket[1]-1])


    (0..keys[0].length-1).each do |i|
      insert_attributs[keys[0][i]] = values[0][i]
    end

    request.insert(db_name)
    request.values(insert_attributs)
    request.run
  end

  def _update_type(args) 
    request  = MySqliteRequest.new
    insert_attributs={}

    set_index = args.index(/[Ss][Ee][Tt]/)
    where_index = args.index(/[Ww][Hh][Ee][Rr][Ee]/)

    db_name = args[0..set_index-1].delete(" ")

    set_columns = CSV.parse(args[set_index+3..where_index-1].delete(" "))
    where_columns = CSV.parse(args[where_index+5..-1].delete(" "))

    request.update(db_name)

    set_columns[0].each do |attributes|
      key, value = attributes.split("=")
      insert_attributs[key] = value
    end
    request.set(insert_attributs)

    where_columns[0].each do |attributes|
      key, value = attributes.split("=")
      request.where(key,value)
    end
    request.run
  end

  def _delete_type(args)
    request  = MySqliteRequest.new

    from_index = args.index(/[Ff][Rr][Oo][Mm]/)
    where_index = args.index(/[Ww][Hh][Ee][Rr][Ee]/)

    db_name = args[from_index+4..where_index-1].strip
    where_columns = CSV.parse(args[where_index+5..-1].delete(" "))

    request.delete
     request.from(db_name)

    where_columns[0].each do |attributes|
      key, value = attributes.split("=")
      puts "key:#{key} value:#{value}"
      request.where(key,value)
    end
    request.run
  end

  def main
    while input_buf = self.run_buf
      if input_buf == "exit"
        break
      end
      
      buf = delete_semicolon(input_buf.strip)

      if buf
      main_type = buf[0..6]
      @args = buf[7..-1]

      @main_statements[main_type][@args]
      end
    end
  end
end

def _run
  myCli  = MyCli.new
  myCli.main
end

_run
# SELECT Player,collage, born FROM nba_players.csv WHERE birth_city=Yorktown;
# SELECT name,college, birth_date FROM nba_player_data.csv WHERE year_end=1995, weight=240;
# SELECT * FROM nba_player_data.csv WHERE year_end=1995;
# INSERT INTO nba_player_data.csv (name,year_start,year_end,position,height,weight,birth_date,college) VALUES (Karl-Anthony Towns,2016,2018,C-F,7-0,244,"November 15, 1995",University of Kentucky);
# UPDATE nba_player_data.csv SET year_start = 2012, year_end = 2019 WHERE weight=240;
# DELETE FROM nba_player_data.csv WHERE year_end=2019;