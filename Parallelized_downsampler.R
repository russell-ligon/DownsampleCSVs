

#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE)

# test if there is at least one argument: if not, return an error
if (length(args)==0) {
  stop("The parent directory containing sub-directories, each with RAW csvs, needs to be identified [1],
       as does the NUMBER OF CORES you want to use to perform this downsampling [2]",
       call.=FALSE)
} 
if (length(args)!=2) {
  stop("The parent directory containing sub-directories, each with RAW csvs, needs to be identified [1],
       as does the NUMBER OF CORES you want to use to perform this downsampling [2]",
       call.=FALSE)
}

library(dplyr)
library(data.table)
library(snow)
library(doParallel)

#Set directory to downsample thermal CSVs to 1htz
# setwd(dir<-"/local/workdir/CHM/TerritoryTrials_chm/1_Raw_Thermal_CSVs")
# setwd(dir<-"J:/EightXEight/T006/")
# setwd(dir<-"K:/8x8/Thermal/T007_rerun/T007_MainDays")

dir<-args[1]
howmancoresshouldiuse<-as.numeric(as.character(args[2]))

downsample<-"y"
#downsample<-"n"



folders<-list.dirs(dir,full.names=TRUE,recursive = FALSE)# 

downsampledfolders<- folders[grep("downsamp",folders)]

folders<-folders[!(folders %in% downsampledfolders)]


downsampleparallel<-function(eachfolder){
  thisfolder<-eachfolder
  FullFiles.FullNames<-list.files(thisfolder,full.names=TRUE,pattern=".csv")# 
  ShortFileNames<-list.files(thisfolder,full.names=FALSE,pattern=".csv")# 
  
  newdir<-paste(last(strsplit(thisfolder,"/")[[1]]),"_downsamp",sep='')
  newdir1<-paste(dir,newdir,sep = "/")
  if (dir.exists(newdir1)){
  } else {
    dir.create(newdir1)
  }
  
  
  if(downsample=="y"){
    print(paste("Downsampling ",dir,sep=''))
    #frameinfo<-lapply(FullFiles.FullNames,function(x) strsplit(x,"2018-"))
    #frameinfo<-strsplit(FullFiles.FullNames,"2018-")
    
    ShortFileNames2<-gsub(".csv","",ShortFileNames)
    ShortFileNames2<-gsub("Record_","",ShortFileNames2)
    frameinfo<-sapply(strsplit(as.character(ShortFileNames2),"_"),"[[",2)#takes 2nd element from each stringsplit
    
    alltimes<-frameinfo
    
    # alltimes<-lapply(frameinfo,function(x) strsplit(x[2],"_"))
    # oknow<-lapply(alltimes,function(x) strsplit(x[[1]],"_"))
    # oknow2<-unlist(lapply(oknow,function(x) x[[1]][c(2)]))
    # oknow3<-unlist(lapply(oknow,function(x) x[[1]][c(1)]))
    
    # 
    # IRstarttimes$day<-sapply(strsplit(as.character(IRstarttimes$file),"_"),"[[",3)#takes 3rd element from each stringsplit
    # IRstarttimes$year<-sapply(strsplit(as.character(IRstarttimes$start_time),"-"),"[[",1)#takes 1st element from each stringsplit
    # IRstarttimes$time<-sapply(strsplit(as.character(IRstarttimes$start_time)," "),"[[",2)#takes 2nd element from each stringsplit
    # 
    # IRstarttimes$t.hour<-as.numeric(as.character(sapply(strsplit(IRstarttimes$time,":"),"[[",1)))
    # IRstarttimes$t.min<-as.numeric(as.character(sapply(strsplit(IRstarttimes$time,":"),"[[",2)))
    # IRstarttimes$t.sec<-as.numeric(as.character(sapply(strsplit(IRstarttimes$time,":"),"[[",3)))
    # 
    # 
    
    
    fileinformation<-data.frame(matrix(unlist(alltimes),nrow=length(alltimes),byrow=T));colnames(fileinformation)<-"V1"
    fileinformation$unique.time<-!duplicated(fileinformation$V1)
    fileinformation$shortname<-ShortFileNames2
    
    # fileinformation$day<-fileinformation$V1
    # fileinformation$tosecond<-oknow2
    # fileinformation$check<-paste(oknow3,oknow2,sep="-")
    # fileinformation$unique.time<-!duplicated(fileinformation$check)      
   
        
    keepers<-FullFiles.FullNames[which(fileinformation$unique.time)]
    
     
    list.of.files<-keepers
    
    # copy the files to the new folder
    new.folder<-newdir1
    file.copy(list.of.files, new.folder)
    

  }
}


thismanycores<-howmancoresshouldiuse
#RB <<- parallel::makeCluster(thismanycores, outfile="")
RB  <<- parallel::makeCluster(thismanycores, outfile=paste("Downsampleout.txt",sep=''))

#  Ccl<-makeCluster(6)
registerDoParallel(RB,cores=thismanycores)
clusterEvalQ(RB, library(data.table)) ## Discard result
print("Starting to run foreach downsampler")


fullon<-length(folders)

#startframe<-1995
#fullon<-startframe+1

main.list<- foreach(viva=1:fullon, 
                    #.packages=c('sp','bigmemory'),
                    .inorder=TRUE) %dopar% {
                      #print(viva)
                      #require(bigmemory)
                      #mainframe2<-attach.big.matrix(datadesc)
                      downsampleparallel(eachfolder=folders[viva])
                    }
parallel::stopCluster(RB)



  







