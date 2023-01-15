# frozen_string_literal: true

module LogEntry
  class Entry
    :lsn # log sequence number
    def initialize(lsn)
      @lsn = lsn
    end
  end

  class Begin < Entry
    :transaction_id

    def initialize(lsn, transaction_id)
      super(lsn)
      @transaction_id = transaction_id
    end

    def to_s
      "#{@lsn} begin #{@transaction_id}\n"
    end
  end

  class Update < Entry
    :transaction_id
    attr_reader :key, :old_value, :new_value # undo # redo

    def initialize(lsn, transaction_id, key, old_value = 'nil', new_value = 'nil')
      super(lsn)
      @transaction_id = transaction_id
      @key = key
      @old_value = old_value
      @new_value = new_value
    end

    # TODO: escape space
    def to_s
      "#{@lsn} update #{@transaction_id} #{@key} #{@old_value} #{@new_value}\n"
    end
  end

  class Commit < Entry
    :transaction_id

    def initialize(lsn, transaction_id)
      super(lsn)
      @transaction_id = transaction_id
    end

    def to_s
      "#{@lsn} commit #{@transaction_id}\n"
    end
  end

  class Abort < Entry
    :transaction_id

    def initialize(lsn, transaction_id)
      super(lsn)
      @transaction_id = transaction_id
    end

    def to_s
      "#{@lsn} abort #{@transaction_id}\n"
    end
  end

  class Checkpoint < Entry
    :pending_transactions

    def initialize(lsn, pending_transactions)
      super(lsn)
      @pending_transactions = pending_transactions
    end

    def to_s
      size = 0
      @pending_transactions.each do |tid, entries|
        size += entries.size
      end

      result = "#{@lsn} checkpoint #{size}\n"
      @pending_transactions.each do |tid, entries|
        entries.each do |entry|
          result << entry.to_s
        end
      end

      result
    end
  end

end

