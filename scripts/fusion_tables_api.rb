require 'httpclient'
require 'json'
require 'log4r'

$log = Log4r::Logger.new("LoggerGlobal")
$log.outputters = Log4r::Outputter.stderr

class FusionTablesAPI
	def initialize()
		@http_client = HTTPClient.new()
		@fusion_table_id = "1wdhbES9O5LaKPq9MhRHmYYleezdNiEix1ftqqQlC"
	end

	def insert_data(params = {})
		raise Exception.new("Missing Fusion Tables API access key/token") if not params["key"] or not params["access_token"]
		begin
			params["tableId"] = @fusion_table_id if not params["tableId"]
			params["uploadType"] = "media" if not params["uploadType"]
			params["isStrict"] = true if not params.has_key?"isStrict"
			content_type = "application/octet-stream" if not params["content_type"]
			request_url = "https://www.googleapis.com/upload/fusiontables/v1/tables/#{params["tableId"]}/import?key=#{params["key"]}&access_token=#{params["access_token"]}&uploadType=#{params["uploadType"]}&isStrict=#{params["isStrict"]}"
			response = @http_client.post(request_url, params["data"], {"Content-Type" => content_type})
			$log.info response.body
		rescue Exception => e
			$log.error "Unable to insert data in the table because of #{e.class}:#{e.message} at line #{__LINE__} in file #{__FILE__}"		
		end	
	end
end

if __FILE__ == $0
	obj = FusionTablesAPI.new
	params = {}
	params["key"] = "AIzaSyC-9WLiooQXJi5nWjFMK2TRH_yS_bka-qU"
	params["access_token"] = "ya29.GwCxhOD7gzC9Yx8AAACZscxf5534MGeqBe5dNASAIDL01QNOeA5-M_Dt8spuow"
	params["data"] = "Bangalore,19-05-2014 17:00:00, 908.8, 1504.8, 0, 25.3, 19, 3, 290, 0.9"
	obj.insert_data(params)	
end
