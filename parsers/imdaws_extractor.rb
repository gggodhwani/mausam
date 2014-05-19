require 'nokogiri'
load_list = ["../scripts/fusion_tables_api.rb"]
load_list.each do |lib|
	require File.expand_path(File.dirname(__FILE__)+"/"+lib)
end

$log = Log4r::Logger.new("GlobalLogger")
$log.outputters = Log4r::Outputter.stderr

class ImdawsParser
	def initialize()
		@http_client = HTTPClient.new()
		@fusion_tables_api = FusionTablesAPI.new()
	end	

	def fetch_response(query_params = {})
		timestamp = Time.now
		query_params["FromDate"] = (timestamp - 500*60*60*24).strftime("%d/%m/%Y") if not query_params["FromDate"] 
		query_params["ToDate"] = timestamp.strftime("%d/%m/%Y") if not query_params["ToDate"]
		query_params["State"] = 12 if not query_params["State"]
		query_params["District"] = "BENGALURU" if not query_params["District"]
		query_params["Loc"] = 827 if not query_params["Loc"]
		url_params = query_params.collect{|key,val| "#{key}=#{val}"}.join('&')
		request_url = "http://www.imdaws.com/WeatherAWSData.aspx?#{url_params}"
		response = @http_client.get(request_url)
	end	

	def is_valid_response(response)
		valid_response = false
		valid_response = true if response.status == 200 and not response.body.chomp.strip.empty?
		return valid_response
	end	

	def parse_response(response)
		parsed_data = ""
		excluded_coloumns = {1 => true, 5 => true, 6 => true, 14 => true, 15 => true, 17 => true} 
		return parsed_data if not is_valid_response(response)
		page_dom = Nokogiri::HTML(response.body)
		page_dom.xpath("//table[@id='DeviceData']/tr[not(contains(.,'STATION NAME'))]").each do |row_node|
			row_data = ""
			coloumn_index = 0		
			row_node.xpath("./td").each do |coloumn_node|
				coloumn_index += 1
				next if excluded_coloumns[coloumn_index]
				if not (coloumn_index == 4 or coloumn_index == 2)
					row_data += "," + coloumn_node.content.chomp.strip.gsub(/ gpm/, "")
				else
					row_data += " " + coloumn_node.content.chomp.strip
				end	
			end
			parsed_data += "\n" + row_data.chomp.strip if not row_data.empty?	
		end	
		return parsed_data.chomp.strip
	end	

	def extract_data(query_params = {})
		query_response = fetch_response(query_params)
		parsed_data = parse_response(query_response)
		$log.info parsed_data
		params = {}
		params["data"] = parsed_data
		params["isStrict"] = false
		@fusion_tables_api.insert_data(params)	
	end	
end

if __FILE__ == $0
	obj = ImdawsParser.new
       	data = obj.extract_data()	
end
