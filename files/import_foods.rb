require 'time'
import 'org.apache.hadoop.hbase.client.HTable'
import 'org.apache.hadoop.hbase.client.Put'
import 'javax.xml.stream.XMLStreamConstants'

def jbytes(*args)
  args.map { |arg| arg.to_s.to_java_bytes }
end

factory = javax.xml.stream.XMLInputFactory.newInstance
reader = factory.createXMLStreamReader(java.lang.System.in)

document = {}
buffer = nil
count = 0

table = HTable.new(@hbase.configuration, 'foods')
table.setAutoFlush(false)

while reader.has_next
  type = reader.next
  if type == XMLStreamConstants::START_ELEMENT
    case reader.local_name
    when 'Food_Display_Row' then document = {}
    when /Food_Code|Display_Name|Portion_Default|Portion_Amount|Calories|Protein|Carbohydrate|Fat/ 
      buffer = []
    end
  elsif type == XMLStreamConstants::CHARACTERS
    buffer << reader.text unless buffer.nil?
  elsif type == XMLStreamConstants::END_ELEMENT
    case reader.local_name
    when /Food_Code|Display_Name|Portion_Default|Portion_Amount|Calories|Protein|Carbohydrate|Fat/
      document[reader.local_name] = buffer.join
    when 'Food_Display_Row'
      key = document['Food_Code'].to_java_bytes
      p = Put.new(key)
      
      document.each do |field, value|
        next if field == 'Food_Code'
        family = 'facts'
        qualifier = field.downcase.gsub('_', '')
        p.add(*jbytes(family, qualifier, value))
      end
      
      table.put(p)
      count += 1
      table.flushCommits() if count % 100 == 0
      puts "#{count} foods imported" if count % 1000 == 0
    end
  end
end

table.flushCommits()
table.close
exit