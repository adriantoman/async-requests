module AsyncRequest
  class Job # rubocop:disable Rails/ApplicationRecord
    include Mongoid::Document
    extend Enumerize

    store_in collection: "jobs", database: "main"

    field :params, type: Array
    field :worker, type: String
    field :uid, type: String
    field :status_code, type: Integer
    field :response, type: String
    field :status
    enumerize :status, in: { :waiting => 1, :processing => 2, :processed => 3, :failed => 4 }, default: :waiting


    def self.create_and_enqueue(worker_class, *params)
      raise ArgumentError("Wroker Class cannot be nil") if worker_class.nil?

      job = Job.new
      job['worker'] = worker_class
      job['params'] = params
      job['status'] = Job.status.find_value(:waiting).value
      job['uid'] = SecureRandom.uuid
      if job.save
        JobProcessor.perform_async(job.id)
      end
      job
    end

    def token
      @token ||= JsonWebToken.encode(id.to_s)
    end

    def successfully_processed!(response, status_code)
      Rails.logger.info("Processing finished successfully for job with id=#{id}")
      update_attributes!(
          status: Job.status.find_value(:processed).value,
          status_code: map_status_code(status_code),
          response: response.to_s
      )
    end

    def processing!
      status = :processing
      Rails.logger.info("Processing job with id=#{id}")
      save!
    end



    def finished?
      status.processed? || status.failed?
    end

    def finished_with_errors!(error)
      Rails.logger.info("Processing failed for job with id=#{id}")
      Rails.logger.info(error.message)
      Rails.logger.info(error.backtrace.inspect)
      update_attributes!(status: :failed, status_code: 500,
                         response: { error: error.message }.to_json)
    end

    private

    def map_status_code(status_code)
      return Rack::Utils::SYMBOL_TO_STATUS_CODE[status_code] if status_code.is_a?(Symbol)
      status_code.to_i
    end
  end
end
