
library(zoo)
library(data.table)
library(snow)
library(doParallel)
library(biganalytics)

library(ff)


library(iotools)

real.width<-640
wrongstringlength<-307200
for(rr in 1:real.width){
  pinto<-seq(rr,wrongstringlength,real.width)
  if(rr>1){
    bean<-c(bean,pinto)
  } else {
    bean<-pinto
  }
}


#PARAMETERS
howmancoresshouldiuse<-20

#DIRECTORY INFO
dir<-"/local/workdir/Ligon/T001"
subdirs<-list.dirs(dir,full.names = TRUE,recursive = FALSE)
#subdirs<-subdirs[c(1,2,4,5)]

for(q in 1:length(subdirs)){
  
  thisdir<-subdirs[q]
  
  newdir<-paste(last(strsplit(thisdir,"/")[[1]]),"_downsamp",sep='')
  newdir1<-paste(dir,newdir,sep = "/")
  if (dir.exists(newdir1)){
  } else {
    dir.create(newdir1)
  }
   
  BigCSVs<-list.files(thisdir,full.names=TRUE,pattern="Summary")# To date, I've named these megaframes "Summary_xxxxxx.csv"
  rownombres<-list.files(thisdir,full.names=TRUE,pattern="Batch")# To date. I've been creating separate files with the frame IDs (from the original ; csvs)
  print(paste("Analyzing folder:",thisdir))
  
  indx<-cbind(seq(1,1951,50),50);colnames(indx)<-c("start","length")
  
	  if(length(grep("T001",thisdir))==1){
		trial<-1
	  } else {
		trial<-0
	  }
  
  for(k in 1:length(BigCSVs)){
    print(paste("Subsampling file",k,"out of",length(BigCSVs)))
    frameIDs<-as.matrix(fread(rownombres[k],sep=",",header=T))#Alphabetically, the frameIDs are item [1] in the list
    
	if(ncol(frameIDs)==2){
		frameIDs<-frameIDs[,2]
	}	
	
    if(trial==1){
      frameIDs<-gsub("new_","",frameIDs)
    }
    
    start.time<-Sys.time()
    ################################
    thismanycores<-howmancoresshouldiuse
    cl <- parallel::makeCluster(thismanycores, outfile=paste("ReverseParallel.txt",sep=''))
    #  cl<-makeCluster(6)
    registerDoParallel(cl,cores=thismanycores)
    clusterEvalQ(cl, library(data.table)) ## Discard result
    print("Starting to run foreach ReverseParallel...")
    pee.matrix<- foreach(rory=1:nrow(indx),
                         .inorder=TRUE) %dopar% {
                           
                           Reverser(rory,whichfile=BigCSVs[k],indx,frameIDs,newdir1,bean)
                           
                         }
    parallel::stopCluster(cl)
    #
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    print(paste("ReverseProcessing of this megastack took",time.taken))
    ###
  }
  print(paste("new files located in:",newdir1))
}

####DOWNSAMPLES DATA to 1HTZ
onesecond.downsampler<-function(datatodownsample=profileplainpee,startingnames=profileplainpee$label){
  
  #print(paste("Downsampling ",dir,sep=''))
  #frameinfo<-lapply(FullFiles.FullNames,function(x) strsplit(x,"2018-"))
  frameinfo<-strsplit(startingnames,"2018-")
  
  alltimes<-lapply(frameinfo,function(x) strsplit(x[2],".csv"))
  oknow<-lapply(alltimes,function(x) strsplit(x[[1]],"_"))
  oknow2<-unlist(lapply(oknow,function(x) x[[1]][c(2)]))
  oknow3<-unlist(lapply(oknow,function(x) x[[1]][c(1)]))
  
  fileinformation<-data.frame(matrix(unlist(alltimes),nrow=length(alltimes),byrow=T));colnames(fileinformation)<-"V1"
  fileinformation$day<-oknow3
  fileinformation$tosecond<-oknow2
  fileinformation$check<-paste(oknow3,oknow2,sep="-")
  fileinformation$unique.time<-!duplicated(fileinformation$check)      
  
  
  #deleters<-fileinformation[which(!fileinformation$unique.time),]
  
  downsampled<-datatodownsample[which(fileinformation$unique.time),]
  downsampled.names<-fileinformation$check[which(fileinformation$unique.time)]
  
  downsampleinfo<-list(downsampled,downsampled.names)
  
  return(downsampleinfo)
}
#downsample just list of names
onesecondgroups<-function(startingnames){
  if(length(grep("2019",startingnames[1]))==0){
    frameinfo<-strsplit(startingnames,"2018-")
  } else {
    frameinfo<-strsplit(startingnames,"2019-")
  }
  
  alltimes<-lapply(frameinfo,function(x) strsplit(x[2],".csv"))
  oknow<-lapply(alltimes,function(x) strsplit(x[[1]],"_"))
  oknow2<-unlist(lapply(oknow,function(x) x[[1]][c(2)]))
  oknow3<-unlist(lapply(oknow,function(x) x[[1]][c(1)]))
  
  fileinformation<-data.frame(matrix(unlist(alltimes),nrow=length(alltimes),byrow=T));colnames(fileinformation)<-"V1"
  fileinformation$day<-oknow3
  fileinformation$tosecond<-oknow2
  fileinformation$check<-paste(oknow3,oknow2,sep="-")
  return(fileinformation$check)
}
#just return downsample indices
onesecond.index<-function(startingnames){
  frameinfo<-strsplit(startingnames,"2018-")
  
  alltimes<-lapply(frameinfo,function(x) strsplit(x[2],".csv"))
  oknow<-lapply(alltimes,function(x) strsplit(x[[1]],"_"))
  oknow2<-unlist(lapply(oknow,function(x) x[[1]][c(2)]))
  oknow3<-unlist(lapply(oknow,function(x) x[[1]][c(1)]))
  
  fileinformation<-data.frame(matrix(unlist(alltimes),nrow=length(alltimes),byrow=T));colnames(fileinformation)<-"V1"
  fileinformation$day<-oknow3
  fileinformation$tosecond<-oknow2
  fileinformation$check<-paste(oknow3,oknow2,sep="-")
  fileinformation$unique.time<-!duplicated(fileinformation$check) 
  return(which(fileinformation$unique.time))
  
}

#Parallelized function for writing new, 1 second csvs
Reverser<-function(rory,whichfile,indx,frameIDs,newdir1,bean){
  
  skipr<-(indx[rory,1]-1)
  miniframe<-as.matrix(fread(whichfile,sep=",",header=FALSE,nrows=indx[rory,2],skip=skipr))
  miniframe<-miniframe[,bean]
  mininames<-frameIDs[c(indx[rory,1]:(49+indx[rory,1]))]
  onesecrate<-miniframe[onesecond.index(mininames),]
  onesecnames<-mininames[onesecond.index(mininames)]
  rm(miniframe);rm(mininames)
  for(teddy in 1:nrow(onesecrate)){
    ok<-onesecrate[teddy,]
    okee<-matrix(ok,nrow=480,ncol=640)#puts vector back into 480x640 spatial location
    secondname<-paste(onesecnames[teddy],".csv",sep='')
    write.table(okee,file=paste(newdir1,secondname,sep="/"),sep=',',col.names=FALSE,row.names = FALSE)
    rm(okee);rm(ok);rm(secondname)
  }
}

# Reverser<-function(rory,subsetframe,subsetnames,newdir1){
#   teddyStrt<-startvals[rory]
#   teddyStp<-stopvals[rory]
#   
#   for(teddy in teddyStrt:teddyStp){
#     ok<-subsetframe[teddy,]
#     okee<-matrix(ok,nrow=480,ncol=640)#puts vector back into 480x640 spatial location
#     secondname<-paste(subsetnames[teddy],".csv",sep='')
#     write.table(okee,file=paste(newdir1,secondname,sep="/"),sep=',',col.names=FALSE,row.names = FALSE)
#   }
# }