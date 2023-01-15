# frozen_string_literal: true

require_relative 'log_entry'

class Database
  :path
  :max_buffer_size

  :buffer_pool
  :log_buffer
  :lsn
  :master # last checkpoint lsn
  :transactions
  :log # log file

  def initialize(path = 'data')
    @path = path
    @max_buffer_size = 4
    @buffer_pool = {}
    @log_buffer = []
    @lsn = 0
    @master = 0
    @transactions = {}

    load_db_from_disk
    recover

    @log = File.open('log', 'a')
  end

  def close
    @log.close
  end

  def next_lsn
    @lsn += 1
  end

  # TODO: 事务 id 单调递增，不允许重复，可以用 timestamp
  # begin 是 ruby 保留关键字，使用时注意
  def begin(tid)
    if @transactions.key?(tid)
      puts "Transaction #{tid} already exists\n"
      return
    end
    entry = LogEntry::Begin.new(next_lsn, tid)
    write_log entry
    @transactions[tid] = [entry]
  end

  def abort(tid)
    unless @transactions.key?(tid)
      puts "Transaction #{tid} does not exist\n"
      return
    end
    write_log LogEntry::Abort.new(next_lsn, tid)
    @transactions[tid].each do |entry|
      next unless entry.is_a? LogEntry::Update

      @buffer_pool[entry.key] = entry.old_value
    end
    @transactions.delete(tid)
  end

  def commit(tid)
    unless @transactions.key?(tid)
      puts "Transaction #{tid} does not exist\n"
      return
    end
    write_log LogEntry::Commit.new(next_lsn, tid)
    @transactions.delete(tid)
  end

  # steal + no-force
  def set(tid, key, value)
    unless @transactions.key?(tid)
      puts "Transaction #{tid} does not exist\n"
      return
    end
    entry = LogEntry::Update.new(next_lsn, tid, key, @buffer_pool[key], value)
    write_log entry
    @transactions[tid] << entry
    @buffer_pool[key] = value
  end

  def get(key)
    @buffer_pool[key]
  end

  def exist?(key)
    @buffer_pool.key?(key)
  end

  def write_log(entry)
    return if @recovering

    @log_buffer << entry
    flush_log if @log_buffer.size >= @max_buffer_size
  end

  def flush_log
    @log_buffer.each do |entry|
      @log << entry.to_s
    end
    @log.flush
    @log_buffer.clear
  end

  def load_db_from_disk
    return unless File.exist?(@path)

    File.open(@path, 'r') do |f|
      @master = f.readline.to_i
      f.each_line do |line|
        next if line.empty?

        key, value = line.split(' ')
        @buffer_pool[key] = value
      end
    end
  end

  def recover
    return if @master <= 0

    puts 'start recovering...'

    @recovering = true
    File.open('log', 'r') do |f|
      never_checkpoint = @master.zero? # 从未落盘的情况下，master 为 0，此时 log 中不存在 checkpoint
      found_checkpoint = never_checkpoint
      entries_size_before_crash = 0
      abort_pending_transactions = -> { @transactions.keys.map { |tid| self.abort(tid) } }

      f.each_line do |line|
        next if line.empty?

        lsn, cmd, *args = line.split(' ')
        next if !found_checkpoint && lsn.to_i < @master

        unless found_checkpoint
          raise 'should got a checkpoint log entry' if cmd != 'checkpoint'

          found_checkpoint = true
          size, * = args
          entries_size_before_crash = size.to_i
          next
        end

        case cmd
        when 'begin'
          tid, * = args
          self.begin tid
        when 'commit'
          tid, * = args
          commit tid
        when 'abort'
          tid, * = args
          abort tid
        when 'update'
          tid, key, old_value, new_values = args
          set tid, key, old_value
        end

        entries_size_before_crash -= 1
        abort_pending_transactions.call if entries_size_before_crash.zero?

      end

      # 如果是首次
      abort_pending_transactions.call if never_checkpoint
    end

    @recovering = false
  end

  def checkpoint
    @master = next_lsn
    write_log LogEntry::Checkpoint.new(@master, @transactions)
    flush_log # flush log before persist dirty pages
    File.open(@path, 'w+') do |f|
      f << "#{@master}\n"
      @buffer_pool.each do |key, value|
        f << "#{key} #{value}\n"
      end
    end
  end

end
