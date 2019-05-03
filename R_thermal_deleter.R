

#Set directory to downsample thermal CSVs to 1htz
setwd(dir<-"J:/EightXEight/T005/tempy")




downsample<-"y"
downsample<-"n"
createscriptfordeleting<-"y"

FullFiles.FullNames<-list.files(dir,full.names=TRUE,pattern=".csv")# 



if(downsample=="y"){
  print(paste("Downsampling ",dir,sep=''))
  #frameinfo<-lapply(FullFiles.FullNames,function(x) strsplit(x,"2018-"))
  frameinfo<-strsplit(FullFiles.FullNames,"2018-")
  
  alltimes<-lapply(frameinfo,function(x) strsplit(x[2],".csv"))
  oknow<-lapply(alltimes,function(x) strsplit(x[[1]],"_"))
  oknow2<-unlist(lapply(oknow,function(x) x[[1]][c(2)]))
  oknow3<-unlist(lapply(oknow,function(x) x[[1]][c(1)]))
  
  fileinformation<-data.frame(matrix(unlist(alltimes),nrow=length(alltimes),byrow=T));colnames(fileinformation)<-"V1"
  fileinformation$day<-oknow3
  fileinformation$tosecond<-oknow2
  fileinformation$check<-paste(oknow3,oknow2,sep="-")
  fileinformation$unique.time<-!duplicated(fileinformation$check)      
  
  
  deleters<-fileinformation[which(!fileinformation$unique.time),]
  
  apply(deleters,1,function(x){
      file.remove(FullFiles.FullNames[grepl(x['V1'],FullFiles.FullNames)])
    }
  )

}
if(createscriptfordeleting=="y"){
  print(paste("Creating file to use for downsampling ",dir,sep=''))
  #frameinfo<-lapply(FullFiles.FullNames,function(x) strsplit(x,"2018-"))
  frameinfo<-strsplit(FullFiles.FullNames,"2018-")
  
  alltimes<-lapply(frameinfo,function(x) strsplit(x[2],".csv"))
  oknow<-lapply(alltimes,function(x) strsplit(x[[1]],"_"))
  oknow2<-unlist(lapply(oknow,function(x) x[[1]][c(2)]))
  oknow3<-unlist(lapply(oknow,function(x) x[[1]][c(1)]))
  
  
  fileinformation<-data.frame(matrix(unlist(alltimes),nrow=length(alltimes),byrow=T));colnames(fileinformation)<-"V1"
  fileinformation$day<-oknow3
  fileinformation$tosecond<-oknow2
  
  fileinformation$check<-paste(oknow3,oknow2,sep="-")
  fileinformation$unique.time<-!duplicated(fileinformation$check)      
  
  
  deleters<-fileinformation[which(!fileinformation$unique.time),]

  deleters.full<-sapply(deleters$V1, function(p) grep(p, FullFiles.FullNames, value=TRUE, ignore.case = TRUE))
  df2<-unlist(lapply(deleters.full, function(k) {strsplit(k,"/")[[1]][length(strsplit(k,"/")[[1]])]}))
  deleters.full<-as.data.frame(df2)
  deleters.full$df2<-as.factor(deleters.full$df2)
  #colnames(deleters.full)<-"Name"
  write.table(deleters.full,file=paste(strsplit(dir,"/")[[1]][4],"txt",sep='.'),
              quote=FALSE,
              row.names = FALSE,col.names = FALSE)
  
  
}
