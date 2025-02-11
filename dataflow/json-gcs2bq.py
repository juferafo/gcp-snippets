import apache_beam as beam
import csv
import json
#cloud
# def run(project, bucket, dataset):
#     argv = [
#         '--project={0}'.format(project),
#         '--job_name=gsntestdf',
#         '--save_main_session',
#         '--staging_location=gs://{0}/ga/staging/'.format(bucket),
#         '--temp_location=gs://{0}/ga/temp/'.format(bucket),
#         '--max_num_workers=8',
#         '--autoscaling_algorithm=THROUGHPUT_BASED',
#         '--runner=DataflowRunner'
#     ]
#     #test_buk/ga/ga_session
#     filename = 'gs://{}/ga/ga_session'.format(bucket)
#
#     pipeline = beam.Pipeline(argv=argv)
#
#     ptransform = (pipeline
#                   | 'Read from GCS' >> beam.io.ReadFromText(filename)
#                   )

def jsonToCsvPart1(fields):
    #header = 'visitNumber, visitId, visitStartTime, date, totals, trafficSource, device, geoNetwork, customDimensions, hits, fullVisitorId, channelGrouping, socialEngagementType'.split(',')
    csvFormat = ''
    fields = json.loads(fields)
    for key in fields.keys() :
            csvFormat += (str(fields[key]) + ",")
    yield csvFormat

class jsonToCsvPart2(beam.DoFn) :
    def __init__(self, delimiter=','):
        self.delimiter =delimiter

    def process(self, fields):
        #Temporary for global
        global fieldsSplit
        fieldsSplit = fields.split(self.delimiter)
        cnt = len(fieldsSplit)/13
        # Temporary for global
        global index
        index = 0
        # 13ê°œ í–‰
        for i in range(0, int(cnt)) :
            tablerow = {
                'visitNumber' : fieldsSplit[index],
                'visitId' : fieldsSplit[index+1],
                'visitStartTime' : fieldsSplit[index+2],
                'date' : fieldsSplit[index+3],
                'totals' : fieldsSplit[index+4],
                'trafficSource' : fieldsSplit[index+5],
                'device' : fieldsSplit[index+6],
                'geoNetwork' : fieldsSplit[index+7],
                'customDimensions' : fieldsSplit[index+8],
                'hits' : fieldsSplit[index+9],
                'fullVisitorId' : fieldsSplit[index+10],
                'channelGrouping' : fieldsSplit[index+11],
                'socialEngagementType' : fieldsSplit[index+12]
            }
            yield tablerow
            index+=13

#local
def run(project, bucket, dataset):
        argv = [
            '--project={0}'.format(project),
            '--job_name=gsntestdf',
            '--save_main_session',
            '--staging_location=gs://{0}/staging/'.format(bucket),
            '--temp_location=gs://{0}/tmp/'.format(bucket),
            '--runner=DirectRunner'
        ]
    #test_buk/ga/ga_session
        events_output = '{}:{}.composer'.format(project, dataset)

        filename = 'gs://{}/data.json'.format(bucket)
        pipeline = beam.Pipeline(argv=argv)
        ptransform = (pipeline
                      | 'Read from GCS' >> beam.io.ReadFromText(filename)
                      #| 'Print' >> beam.Map(print)
                      | 'Convert CSV' >> beam.FlatMap(jsonToCsvPart1)
                      | 'Make Table Row' >> beam.ParDo(jsonToCsvPart2(','))
                    )
        #visitNumber, visitId, visitStartTime, date, totals, trafficSource, device, geoNetwork, customDimensions, hits, fullVisitorId, channelGrouping, socialEngagementType
        schema = 'visitNumber:string,visitId:string,visitStartTime:string,date:string,totals:string,trafficSource:string,device:string,geoNetwork:string,customDimensions:string,'\
                 'hits:string,fullVisitorId:string,channelGrouping:string,socialEngagementType:string'

        (ptransform
         | 'events:out' >> beam.io.WriteToBigQuery(
                    events_output, schema=schema,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
         )


        pipeline.run()

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Run pipeline on the cloud')
    parser.add_argument('-p', '--project', help='Unique project ID', required=True)
    parser.add_argument('-b', '--bucket', help='Bucket where your data were ingested', required=True)
    parser.add_argument('-d', '--dataset', help='BigQuery dataset', default='gs_session_2')

    args = vars(parser.parse_args())

    print("Correcting timestamps and writing to BigQuery dataset {}".format(args['dataset']))

    run(project=args['project'], bucket=args['bucket'], dataset=args['dataset'])
