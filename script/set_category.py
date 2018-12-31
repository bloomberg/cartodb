#! /bin/python
######################################################################################################################################################################################
#### Automated updates for categories
#### Uses - https://bbgithub.dev.bloomberg.com/datavis-cartodb/cartodb-docs/blob/master/playbooks/Categories.md
#### Expects csv file as input argument
######### CSV file format should be e.g.
######### dataset_name,category_name
######### ......
######### dataset_name should be the table name as it appears in data library
######### category_name should be the name of the category populated in meta-database table - visualization_categories.name
######### Run it as cartodb id on any editor host as full_path/set_category.py full_csv_path/csv_filename 
######### e.g. Run it as - /bb/datavis/cartodb/embedded/cartodb/script/set_category.py /home/cartodb/category/dataset_test.csv >/home/cartodb/category/dataset_test.out
########  Purpose of output file is to verify the run was ok. Remove the csv and output files after verification
######################################################################################################################################################################################
import csv
import sys
import subprocess

def apply_categories(csv_filename):
        try:
                f = open(csv_filename, 'rb')
                reader = csv.reader(f)
                for row in reader :
                        #print row[0] + "   " + row[1]
                        cmd_to_run="cd /bb/datavis/cartodb/embedded/cartodb; bundle exec rake cartodb:remotes:set_dataset_category_by_name['" + row[0] + "','" + row[1] +"']"
			print cmd_to_run
			subprocess.check_call([cmd_to_run],shell=True)

                f.close()
        except:
                print "Could not process apply_categories"
                sys.exit(1)
        return 0


if __name__ == '__main__':

        if len(sys.argv) != 2 :
                print "Need input comma separated CSV file having dataset_name, category_name!"
                sys.exit(1)
        else:
                #call apply_categories
                apply_categories(sys.argv[1])
######################################################################################################################################################################################
