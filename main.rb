# frozen_string_literal: true

require_relative 'database'

def main
  db = Database.new
  loop do
    cmd, *args = gets.split(' ')
    process_command db, cmd, args
  end
end

def process_command(db, cmd, args)
  case cmd
  when 'begin'
    # begin <tid>
    if args.empty?
      puts 'transaction id required'
      return
    end
    tid, * = args
    db.begin tid
  when 'set'
    # set <tid> <key> <value>
    tid, key, value = args
    if key.nil? || key.empty?
      puts 'key required'
      return
    end
    if value.nil? || value.empty?
      puts 'value required'
      return
    end
    db.set(tid, key, value)
  when 'get'
    # get <key>
    if args.empty?
      puts 'key required'
      return
    end
    key, * = args
    puts(db.exist?(key) ? db.get(key) : '<nil>')
  when 'commit'
    # commit <tid>
    if args.empty?
      puts 'transaction id required'
      return
    end
    tid, * = args
    db.commit(tid)
  when 'abort'
    # abort <tid>
    if args.empty?
      puts 'transaction id required'
      return
    end
    tid, * = args
    db.abort(tid)
  when 'checkpoint'
    db.checkpoint
  when 'exit', 'quit'
    db.close
    exit
  else
    raise ArgumentError, "Invalid command #{cmd}"
  end
end

main if __FILE__ == $PROGRAM_NAME
