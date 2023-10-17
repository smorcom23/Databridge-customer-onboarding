#!/bin/sh

##################################################
#  Author: Mahantesh Patil
#  Date: July 2023
#  Purpose: This script is used to create new informatica jobs using dbmicli commands. 
#           cli command enable to create new jobs based on al already existing job. This is the ideal toolkit for recreating jobs
#           with new input and output connetion for every newly onboarded tenant.
#
#  Usage: The script expect one mandatory input arhugement and two optional ones. The mandatory argumane is customer_name,
#	  and the optional ones are deploy and execute.
#		
#	  Arguments should be passwed with no space between variable name and value, eg: customer_name=test1
#	  deploy=true . Customer_name value could be any string, were as the only values thr other two optional
#	  argumanets take is TRUE/true.
#
##################################################


export DBMIPOD=dm-em.informaticacloud.com
export DBMIUSER=<username>
export DBMIPASSWORD=<password>
export JAVA_HOME=/opt/infa/infaagent/jdk
export PATH=$PATH:/opt/infa/infaagent/jdk/bin
export current_environment=perf


echo "Checking input arguments "

if [[ "$#" -eq 0 ]]
then 
	echo "Expected atleast one argument received none"
	echo "Exiting the script "
	exit 1
else
	echo "Here are the input arguments passed to the script:"
fi

for ARGUMENT in "$@"
do
   KEY=$(echo $ARGUMENT | cut -f1 -d=)

   KEY_LENGTH=${#KEY}
   VALUE="${ARGUMENT:$KEY_LENGTH+1}"

   export "$KEY"="$VALUE"
  
  echo "${KEY}=${VALUE}"
done


if [[ -z $customer_name ]]
then echo "customer_name argument is empty, this is mandatory argument. Please pass the value in next run. Exiting the script"
		exit 1
fi

for job_type in case sub lib common oper
do

	echo "Constructing dbmicli command for job type: $job_type"
	cp ./yml/template.yml temp_template.yml


	sed -e "s/\${job_type}/$job_type/" -e "s/\${tenant}/${customer_name}/" -e "s/\${current_environment}/${current_environment}/" temp_template.yml > ${job_type}_temp_template.yml
	dbmicli_create_newtask_part1="./dbmicli.sh task createFrom --taskName="
	dbmicli_taskname="lsmv_${job_type}_to_lsdb_stg_rpl"
	dbmicli_create_newtask_part2=' --taskLocation='databridge\\base_code' --taskType=DBMI --override='
	dbmicli_create_newtask_part3=${job_type}_temp_template.yml
	dbmicli_deploy_job_part1="./dbmicli.sh task deploy --taskName="
	dbmicli_deploy_job_part2=" --taskLocation="
	newtask_location="databridge\\${customer_name}"
	newtask_name="${customer_name}_${dbmicli_taskname}"
	cdc_script_part1="./dbmicli.sh task cdc --taskName="
	cdc_script_part2=" --scope=all --execute"
	
	create_task_final_command=${dbmicli_create_newtask_part1}${dbmicli_taskname}${dbmicli_create_newtask_part2}${dbmicli_create_newtask_part3}

	echo "Create job command: ${create_task_final_command}"
	echo "Running command to create informatica job"

		sh $create_task_final_command
	
		
	if [[ $? -ne 0 ]]
		then
			echo "dbmicli command to create new job failed, exiting the script"
			exit 1
	fi


	cdc_script_final_command=${cdc_script_part1}${newtask_name}${dbmicli_deploy_job_part2}${newtask_location}${cdc_script_part2}

	echo "CDC script constructed command: ${cdc_script_final_command}"
	echo "Executing CDC script to enable database CDC option on the source tables"
		
		sh ${cdc_script_final_command}
		

	if [[ $? -ne 0 ]]
		then
			echo "dbmicli command failed executing CDC script, exiting the script"
			exit 1
	fi


	if [[ "${deploy^^}" == 'TRUE' ]]
		then 
		deploy_final_command=${dbmicli_deploy_job_part1}${newtask_name}${dbmicli_deploy_job_part2}${newtask_location}
		echo "Deploy final command: ${deploy_final_command}"
		echo "Executing deploy command"
			
			sh ${deploy_final_command}

			if [[ $? -ne 0 ]]
				then
					echo "dbmicli command failed to deploy job, exiting the script"
						exit 1
			fi
	fi


		echo "-------------------------------------------------------------------------------------------------------------------------"

done

