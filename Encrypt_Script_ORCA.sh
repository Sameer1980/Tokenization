mail_list1='Aman.Gupta1@chubb.com,sukanya.thekkevalape@chubb.com,fpandit@chubb.com,lcepak@chubb.com,schivukula@chubb.com,rjha@chubb.com','sseipel@chubb.com'
mail_list='Chandni.Chandwani@Chubb.com,Samir.Banerjee@Chubb.com,supraja.pareddy@chubb.com'
wd='/cbdl0apap04/opt/data/powercenter/cpi/Scripts/clnt/MDM_python'

cd $wd
python3.7 -W ignore token_files_incremental_ORCA.py && mail -s "PROD:Batch Tokenisation ORCA"::"Results" $mail_list  <$wd/MDM_Tokenization_ORCA.log
if [ "$?" != 0 ]
    then echo "Program has failed!!"
         mail -s "Task failed::PROD:Batch Tokenisation ORCA" $mail_list <$wd/MDM_Tokenization_ORCA.log
         exit
   fi


