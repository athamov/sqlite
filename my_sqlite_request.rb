require 'csv'

class MySqliteRequest
    def initialize
        @type_request     = :none
        @select_column    = []
        @where_param      = []
        @insert_attribut  = {}
        @db_name          = nil
        @order            = :asc

    end

    def from(db_name)
        @db_name = db_name
        self
    end

    def select(columns)
        if(columns.is_a?(Array))
            @select_column += columns.collect { |row| row.to_s}
        else
            @select_column << columns.to_s
        end
        self._setTypeOfRequest(:select)
        self
    end

    def where(column_name, criteria)
        @where_param << [column_name, criteria]
        self
    end

    def join(column_on_db_a, filename_db_b, column_on_db_b)
        self
    end

    def order(order, column_name)
        self
    end

    def insert(db_name)
        self._setTypeOfRequest(:insert)
        @db_name = db_name
        self
    end

    def values(data)
        @insert_attribut = data 
        self
    end

    def update(db_name)
        self._setTypeOfRequest(:update)
        @db_name = db_name
        self
    end

    def set(data)
        @insert_attribut = data
        self
    end

    def delete
        self._setTypeOfRequest(:delete)
        self
    end

    def up_del_file(result)
        CSV.open(@db_name, "w", :headers => true) do |n| 
            n << result[0].to_hash.keys
            result.each do |nba|
                n << CSV::Row.new(nba.to_hash.keys, nba.to_hash.values)
            end
        end
    end

    def print_insert
        puts "Insert Attributes #{@insert_attribut}"
    end

    def print_select(result = nil)
        puts "Select Attributes #{@select_column}"
        puts "Where Attributes #{@where_param}"
        if !result
            return
        end
        if result.length == 0
            puts "No information found"
        else
            puts result.first.keys.join(' | ')
            len = result.first.keys.join(' | ').length
            puts "-_" * len
            result.each do |line|
                puts line.values.join(' | ')
            end
            puts "-_" * len
        end
    end

    def print
        puts "Type of request #{@type_request}"
        puts "Table name #{@db_name}"
        if(@type_request == :select)
            print_select()
        elsif(@type_request == :insert)
            print_insert
        end
    end

    def run
        print
        if(@type_request == :select)
            param = _run_select_
            print_select(param)
        elsif(@type_request == :insert)
            _run_insert_
        elsif(@type_request == :update)
            update = _run_update_
            up_del_file(update)
        elsif(@type_request == :delete)
            delete = _run_delete_
            up_del_file(delete)
        end
    end

    def _setTypeOfRequest(new_type)
        if(@type_request == :none || @type_request == new_type)
            @type_request = new_type
        else
            puts 'Invalid request'
        end
    end

    def _run_select_
        csv = CSV.parse(File.read(@db_name), headers: true)
        result = []
        if(@select_column != [] && @where_param != [])
            csv.each do |row|
                @where_param.each do |where_attribute|
                    if(row[where_attribute[0]] == where_attribute[1])
                        if(@select_column[0] == "*")
                            result << row.to_hash
                        else
                        result << row.to_hash.slice(*@select_column)
                        end
                    end
                end
            end
        elsif(@select_column != [] && @where_param == [])
            csv.each do |row|
                @select_column.each do |sel_col|
                    if row[sel_col]
                        result << row.to_hash.slice(*@select_column)
                    else
                        if(@select_column == "*")
                            result << row.to_hash
                        end
                    end
                end
            end
        end
        result
    end

    def _run_insert_
        File.open(@db_name, File::RDWR|File::CREAT, 0644) {|f|
            f.flock(File::LOCK_EX)
            value = f.read
            f.rewind
            insert = @insert_attribut.values.join(',')
            f.write("#{value}#{insert}")
            f.flush
            f.truncate(f.pos)
          }
        # File.open(@db_name, 'a') do |val|
        #     val.puts @insert_attribut.values.join(',')
        # end 
    end

    def _run_update_
        csv = CSV.parse(File.read(@db_name), headers: true)
        result = []
        csv.each do |row|
            @where_param.each do |header, body|
                if body == row[header]
                    @insert_attribut.each do |header, body|
                        row[header] = body
                    end
                    result << row
                else
                    result << row
                end
            end
        end
        return result
    end

    def _run_delete_
        csv = CSV.parse(File.read(@db_name), headers: true)
        result = []
        csv.each do |row|
            @where_param.each do |header, body|
                if body == row[header]
                    next
                else
                    result << row
                end
            end
        end
        return result
    end 
end

