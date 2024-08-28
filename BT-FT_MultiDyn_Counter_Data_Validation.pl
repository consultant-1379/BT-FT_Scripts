#!/usr/bin/perl

use strict;
#use warnings;
no warnings 'uninitialized';
use List::Util;
use DBI;
use Data::Dumper;

my $result;
my $pass=0;
my $fail=0;
my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";
my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$Toponodename,$hyphen1,$month,$hyphen2,$date,$space,$colon1,$colon2,$datetimeepfg,$package);

##################################################################################
#             The DATETIME value for the FILENAME of the HTML LOGS               #
##################################################################################

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) =localtime(time);
  $mon++;
  $year=1900+$year;
my $datenew =sprintf("%4d%02d%02d%02d%02d%02d",$year,$mon,$mday,$hour,$min,$wday);

############################################################
# GET TIMESTAMP
# This is a utility 
sub getTime{
  my ($sec,$min,$hour,$mday,$mon,$year,$wday, $yday,$isdst)=localtime(time);
  return sprintf "%4d-%02d-%02d %02d:%02d:%02d\n", $year+1900,$mon+1,$mday,$hour,$min,$sec;
}

############################################################
# GET THE HTML HEADER
# This is a utility for the log output file in HTML 
sub getHtmlHeader{
my $testCase = shift;
return qq{<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<title>
$testCase
</title>
<STYLE TYPE="text/css">
h3{font-family:tahoma;font-size:12px}
body,td,tr,p,h{font-family:tahoma;font-size:11px}
.pre{font-family:Courier;font-size:9px;color:#000}
.h{font-size:9px}
.td{font-size:9px}
.tr{font-size:9px}
.h{color:#3366cc}
.q{color:#00c}

</STYLE>
</head>
<body>
};
}

############################################################
# GET HTML TAIL
# This is a utility for the log output file in HTML 

sub getHtmlTail{
return qq{
</table>
<br>
</body>
</html>
};

}
############################################################
# WRITE HTML
# This is a utility for the log output file in HTML 

sub writeHtml{

my $server = shift;
my $out    = shift;
open(OUT," > $LOGPATH/$server.html");
print OUT $out;
close(OUT);
return "$LOGPATH/$server.html\n";

}

#######################################################################
# ExecuteSQL
# This will give the resultset data for the queries passed

sub executeSQL{
        my $dbname = $_[0];
        my $port = $_[1];
        my $cre = $_[2];
		my $cre1 = $_[3];
        my $arg = $_[4];
        my $type = $_[5];
        #print "executeSQL : $arg  \n\n";
                
		my $connstr = "ENG=$dbname;CommLinks=tcpip{host=localhost;port=$port};UID=$cre;PWD=$cre1";
		#print $connstr;
				my $dbh = DBI->connect( "DBI:SQLAnywhere:$connstr", '', '', {AutoCommit => 1} ) or warn $DBI::errstr;
                my $sel_stmt=$dbh->prepare($arg) or warn $DBI::errstr;

        if ( $type eq "ROW" ) {
            $sel_stmt->execute() or warn $DBI::errstr;
                my @result = $sel_stmt->fetchrow_array();
                $sel_stmt->finish();
                #$dbh->disconnect;
                return @result;
                }       
        elsif ( $type eq "ALL" ) {
                $sel_stmt->execute() or warn $DBI::errstr;
                my $result = $sel_stmt->fetchall_arrayref();
                $sel_stmt->finish();
                return $result;
        }
        $dbh->disconnect;
		}
###############################################################################																			

########################################################################################################
#  Verify_MultiDynCounters will verify whether the data is matching with the EPFG input file to DB data.
#  Script was developed and introduced for EQEV-121950 Jira in 23.4.7_ENIQ_TP(23.4) sprint version.
########################################################################################################
sub Verify_MultiDynCounters{
	
	my %hash_for_tagID;
	my %hash_for_counter;
	my %hash_for_dataformat;
	my %hash_for_counter_dataformat;
	my %hash_for_counter_key_dataformat;
	my %Counter_Type;
	my $table_name;
	my $table_dyn;
	my $table_dyn_V;
	my $query_for_dwhdb;
	my $table_printing;
	my ($not,$tagID_table);
	my $counter_printing;
	our %Counters_List_array;
	my $result_from_dwhdb;
	my $moid_printing;
	my $counters_size;
	my $col_count;
	my $final_counter_name;
		
	my %Dyncounter_epfg;

	my %dbl_counters_epfg;
	my %dbl_counters_flex_epfg;
	my %sngl_counters_epfg;
	my %sngl_counters_flex_epfg;
	my %Counters_epfg;
	
	my %Multi_counters_epfg;
	my $flag;
	my @epfg_counter_and_data;
	my ($tag,$tagid);
	my $Dyncountername;
	my $counter_name;
	my $flex_name;
	my ($cntr,$keyy,$kval,$keyy2,$k2val,$keyy3,$k3val);
	my $interface_name;
	
	open(Pass, "<Passwords.txt");
	my $name = do {local $/; <Pass> };
	my ($DCPass,$DwhrepPass,$EtlrepPass) = split("\n",$name);
	#print "DCPass: $DCPass , DwhrepPass: $DwhrepPass , EtlrepPass: $DwhrepPass";
	
	my $file = 'data_new.txt';
	open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
	my @array = <FILE>;
	my $line = shift @array;
	chomp $line;	
	#print "$line\n";
	
	($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$Toponodename) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.*)/;
	@array = split(",",$Nodename);
	$Nodename = $array[0];
	#print "$tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$Toponodename";
	($year,$hyphen1,$month,$hyphen2,$date,$space,$hour,$colon1,$min,$colon2,$sec) = $datetime =~ m/(\d+)(-)(\d+)(-)(\d+)( )(\d+)(:)(\d+)(:)(\d+)/;
	$datetimeepfg = "$year$month$date.$hour$min";
	#print "$datetimeepfg";
	$package = $tp_name.":((".$build."))";
	#print "$package\n";
	$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left colspan=7><font color=blue><font size=4px><b>$package</b></font></font></td></tr></table>";

	my $query_from_repdb_counter = "select substr(substr(DataItem.DATAFORMATID,charindex('))',DataItem.DATAFORMATID)+3),1,charindex(':',substr(DataItem.DATAFORMATID,charindex('))',DataItem.DATAFORMATID)+3))-1) as table_name, DATANAME as counter_name, DATAID as dataformat from DataItem where dataformatid like '%".$package."%DYN%' and DATANAME in (select dataname as counter from MeasurementCounter  where MeasurementCounter.TYPEID like '%".$package."%DYN%') order by table_name";
	my $result_from_repdb_counter = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_from_repdb_counter,"ALL");
	my $query_from_repdb_table = "select substr(substr(DATAFORMATID,charindex('))',DATAFORMATID)+3),1,charindex(':',substr(DATAFORMATID,charindex('))',DATAFORMATID)+3))-1) as tab, TAGID from DefaultTags where dataformatid like '%".$package."%DYN%'  order by tab";
	my $result_from_repdb_table = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_from_repdb_table,"ALL");
	
	undef %hash_for_tagID;
	undef %hash_for_counter;
	undef %hash_for_dataformat;
	undef %hash_for_counter_dataformat;
	undef %hash_for_counter_key_dataformat;
	
	for my $data_from_repdb_table (@$result_from_repdb_table) {
			
			my ($table, $tagID) = @$data_from_repdb_table;
			my $rep_query = "select followjohn from MeasurementType where typename like '$table' and TYPEID like '$package:%';" ;
			my ($result_repdb_1)= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$rep_query,"ALL");
			#table check if deprecated.
			my $Pattern_flag=0;
			for my $r1 (@$result_repdb_1){
				my $temp1=@$r1[0];
				if($temp1 == "0"){
					$Pattern_flag=1;
				}else{
					my $rep_query_2 = "select description from MeasurementType where typename like '$table' and TYPEID like '$package:%';" ;
					my ($result_repdb_2)= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$rep_query_2,"ALL");
				
					for my $r2 (@$result_repdb_2){
						my $temp=@$r2[0];
					
						my @desc_patterns   = ("Deprecated:(To be deprecated in n-8 release)","Deprecated since", "Deprecated,since", "Deprecated,Since", "Deprecated Since", "Deprecated. Since", "Deprecated. since", "Deprecated only in", "Deprecated in", "Deprecated: Since", "Deprecated:Since", "Deprecated.", "Deprecated,");
										
						for my $pattern (@desc_patterns) {	
							if($temp =~ m/\w*$pattern\w*/){
								$Pattern_flag=1;
								last;
							}
						}
					}
				}
			}

			if($Pattern_flag == '0'){
				$hash_for_tagID{$tagID} = $table;
			}
	}
	#print Dumper(\%hash_for_tagID);
	
	for my $data_from_repdb_counter (@$result_from_repdb_counter) {														
		my ($table, $counter, $dataformat) = @$data_from_repdb_counter;
		
		push @{$Counters_List_array{$table}}, $counter;
		
		my $rep_query_2 = "select description,COUNTERTYPE from MeasurementCounter where TYPEID like '$package:$table' and dataname like '$counter';" ;
		my ($result_repdb_2)= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$rep_query_2,"ALL");
				
		for my $r2 (@$result_repdb_2){
			my $temp=@$r2[0];
			my $Pattern_flag=0;
			my @desc_patterns   = ("Deprecated:(To be deprecated in n-8 release)","Deprecated since", "Deprecated,since", "Deprecated,Since", "Deprecated Since", "Deprecated. Since", "Deprecated. since", "Deprecated only in", "Deprecated in", "Deprecated: Since", "Deprecated:Since", "Deprecated.", "Deprecated,");
										
			for my $pattern (@desc_patterns) {	
					
				if($temp =~ m/\w*$pattern\w*/){
					$Pattern_flag=1;
					last;
				}
			}
			if($Pattern_flag == '0'){
				my $new_counter = "cast (".$counter." as Numeric(20,0))";
				push @{$hash_for_counter{$table}}, $new_counter;
				push @{$hash_for_dataformat{$table}}, $dataformat;
				push @{$hash_for_counter_dataformat{$counter}}, $dataformat;
				$Counter_Type{$counter} = @$r2[1];
			}
		}
	}
	
	# foreach my $key_for_table ( keys %hash_for_tagID ) {
			
			# undef $table_name;
			# $table_name = $key_for_table."_RAW";
			# #print "table_name :$table_name\n";
			# chomp($table_name);
			# my $query_array_for_counters;
			# my $array_for_dataformat;
			
			# $query_array_for_counters = join(',', @{$hash_for_counter{$key_for_table}});
			# $array_for_dataformat = join(',', @{$hash_for_dataformat{$key_for_table}});
	# }
	my $query_intf = "select INTERFACENAME from datainterface where INTERFACENAME in (select INTERFACENAME from interfacetechpacks where techpackname like '$tp_name')";
	#print "$query\n";
	my $result_from_repdb = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_intf,"ALL");
	for my $r ( @$result_from_repdb ) 
	{
		#print "interface name :@$r[0]\n";
		my $query = "select COLLECTION_SET_ID from META_COLLECTION_SETS WHERE COLLECTION_SET_NAME LIKE '".@$r[0]."-eniq_oss_1'";
		
		my @result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
		
		#$query ="select substr(action_contents_01,charindex('processedDir',action_contents_01)+39,charindex('/processed',action_contents_01)-
		#(charindex('processedDir',action_contents_01)+39)) procdir, substr(action_contents_01,charindex('multiDynMaskConfig.1=',action_contents_01)+21,
		#charindex('::',action_contents_01)-(charindex('multiDynMaskConfig.1=',action_contents_01)+21)) config1,substr(action_contents_01,
		#charindex('multiDynMaskConfig.2=',action_contents_01)+21, charindex('::',
		#action_contents_01) -(charindex ('multiDynMaskConfig.1=',action_contents_01)+14)) config2 from meta_transfer_actions where collection_set_id =
		#".$result_from_repdb1[0]." and action_type like 'parse' and action_contents_01 like '%dyn%'";
		#-----------------
		$query = "select substr(action_contents_01,charindex('processedDir',action_contents_01)+39,charindex('/processed',action_contents_01)-
(charindex('processedDir',action_contents_01)+39)) procdir, action_contents_01 from meta_transfer_actions where 
collection_set_id = ".$result_from_repdb1[0]." and action_type like 'parse' and action_contents_01 like '%dyn%'";
		#-----------------
		
		@result_from_repdb1 = executeSQL("repdb",2641,"etlrep",$EtlrepPass,$query,"ROW");
		
		my $dir = $result_from_repdb1[0];
		
		my @Temp = split("\n",$result_from_repdb1[1]);
		#print Dumper(\@Temp);
		
		if($dir ne ''){
			
			my $config1;
			my $config2;
			
			my $Patt = "x3GPPParser.multiDynMaskConfig";
			my $Patt1 = "x3GPPParser.multiDynVectorMaskConfig";
			@Temp = grep {/^$Patt|^$Patt1/i} @Temp;
			#print Dumper(\@Temp);
			$interface_name = @$r[0];
			#print "interface_name :$interface_name\n";
			if ($tp_name eq 'DC_E_NR'){
				my $in_path = "/eniq/home/dcuser/epfg/txt/$dir";
				
				if(opendir ( DIR, $in_path )){
				
					while( my $intf_file = readdir(DIR)){
						my ($correct_intf_file) = $intf_file =~ m/(CountersTxt_$datetimeepfg\_$Nodename\.csv)/;
		
						if ($intf_file eq $correct_intf_file) {
							
							$result.=qq{<body bgcolor=GhostWhite></body><center><table id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
							
							$result .= "<tr><td align=left><font color=black><font size=3px><b>Interface Name:</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$interface_name</font></font></td></tr>";
							
							
							
							my $csvfile_from_epfg = "/eniq/home/dcuser/epfg/txt/$dir/$correct_intf_file";
							#print "csvfile_from_epfg :$csvfile_from_epfg\n";
							open (EPFG_FILE , '<', $csvfile_from_epfg ) or die "ERROR: Couldn't open file for writing '$csvfile_from_epfg' $! \n";
							
							while (<EPFG_FILE>) 
							{
								chomp;	
								my %nrml_counters_epfg;
						my @multidyn_db_keys;
						my @multidyn_keys;
						my @uniq_multidyn_keys;
								@epfg_counter_and_data = split(/;/, $_);
								($tag,$tagid) = split(":",$epfg_counter_and_data[0]);
								my ($mo,$moid);
								
								# if($tagid eq "NRCellCU" or $tagid eq "NRCellDU" or $tagid eq "GNBCUUPFunction" or $tagid eq "GNBCUCPFunction"){
								#print Dumper(%hash_for_tagID);
										print "$tagid\n";
										$table_dyn_V = $hash_for_tagID{$tagid."_Events_FLEX_DYN_V"};
										$table_dyn = $hash_for_tagID{$tagid."_Events_FLEX_DYN"};
										
									if($table_dyn ne '' or $table_dyn_V ne '')
									{

										($mo,$moid) = $epfg_counter_and_data[1] =~ m/(MOID:)(.*)/;
										$moid_printing = $moid;
										my $count = @epfg_counter_and_data;
										##print "epfg_counter_and_data - Count:$count\n";
										
											#print "Epfg Data : $count\n";
										
										for(my $i=2;$i<$count;$i++)
										{
											my $flagg = 0;
											my ($DYNcounter,$value) = split(":",$epfg_counter_and_data[$i]);
											$Dyncounter_epfg{$DYNcounter} = $value;
											
											if ($DYNcounter =~ m/\w*_\w*/){
												my @fields = split /_/, $DYNcounter;
												$Dyncountername = $fields[0];
												$flex_name=$fields[1];
											}else{
												$Dyncountername = $DYNcounter;
												$flex_name='';
											}
											#print Dumper(\@Temp);
											#last;
											foreach my $config (@Temp)
											{
												#print "$config\n";
												$config2 = $config;
												$config2 =~ s#\\\\#\\#g;
												#print "$config\n";
												($config2,$config1) = split('=',$config2);
												my ($config1,@TempConfig) = split('::',$config1);
												#print "$config1\n";
												my @A = $Dyncountername =~ qr/$config1/;
												#print Dumper(\@A);
												if(scalar(@A) eq 5)
												{
												$Counters_epfg{$A[0]."-".$A[1].$A[2].$A[3].$A[4]."-".$flex_name} = "$value";
												push(@multidyn_keys,$A[1],$A[3]);
												$Multi_counters_epfg{$DYNcounter} = $value;
												$flagg=1;
												last;
												}
												elsif(scalar(@A) eq 3)
												{
												$Counters_epfg{$A[0]."-".$A[1].$A[2]."-".$flex_name} = "$value";
												push(@multidyn_keys,$A[1]);
												$Multi_counters_epfg{$DYNcounter} = $value;
												$flagg=1;
												last;
												}
												
											}
											if($flagg eq 0)
												{
													$nrml_counters_epfg{$DYNcounter} = $value;
													
												}
										}

									#------------- removing duplicate keys from the array  -------#										
										sub uniq {
											my %seen;
											grep !$seen{$_}++, @_;
										}
										my @uniq_multidyn_keys = uniq(@multidyn_keys);

									#------------- end removing duplicate keys from the array  -------#		

										my $Dyn_key_array;
										foreach my $k (@uniq_multidyn_keys){
											$Dyn_key_array .= "'$k'".",";
										}
										chop($Dyn_key_array);
									if($table_dyn ne '')
									{
										$result .= "<tr><td align=left><font color=black><font size=3px><b>Table Name:</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$table_dyn</font></font></td></tr>";
										
										$result .= "<tr><td align=left><font color=FF00FF><font size=2px><b>CounterName</b></font></font></td><td align=left><font color=FF00FF><font size=2px><b>FlexName</b></font></font></td><td align=left><font color=FF00FF><font size=2px><b>Dyn Key</b></font></font></td><td align=left><font color=FF00FF><font size=2px><b>Moid</b></font></font></td><td align=left><font color=FF00FF><font size=2px><b>Result</b></font></font></td></tr>";
										print "$table_dyn\n";
										my $query_for_dynkeys = "select distinct dataname,dataid from dataitem where dataformatid like '%$package%$table_dyn:%' and dataid in ($Dyn_key_array)";
								
										my $result_for_dynkeys = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_for_dynkeys,"ALL");
										my @Db_keys;
										for my $r2 (@$result_for_dynkeys){
											my $temp=@$r2[0];
											push(@multidyn_db_keys,",".$temp);
											push(@Db_keys,@$r2[1]);
										}
									
									#------------- getting data from dwhdb ----------#
#=begin
										my $counters_for_query = join(',', @{$hash_for_counter{$table_dyn}});
										
										undef $table_name;
										$table_name = $table_dyn."_RAW";
									# if($table_name eq 'DC_E_NR_EVENTS_NRCELLCU_FLEX_DYN_RAW')
									# {
										my $queryy = "select $counters_for_query,FLEX_FILTERNAME @multidyn_db_keys from $table_name where DATETIME_ID = '$datetime' and moid like '$moid' and $Nodenamekey like '$Nodename'";
										
										#print "$queryy\n";
										
										undef $result_from_dwhdb;
										$result_from_dwhdb = executeSQL("dwhdb",2640,"dc",$DCPass,$queryy,"ALL");
										our @Counters_List = @{$Counters_List_array{$table_dyn}};
										$counters_size = @{$Counters_List_array{$table_dyn}};
										my $k=0;
									#-------------Start of processing the outout data of dwhdb -------------#
										for my $r (@$result_from_dwhdb)
										{								
											my $col_count = @$r;
											our $counters_size = @Counters_List;
											my $i;
											my $PostAppend = '';
											my $count1=0;
											for(my $j=$counters_size+1;$j<$counters_size+scalar(@Db_keys)+1;$j++)
											{
												if(@$r[$j] ne '')
												{
													$PostAppend = $PostAppend.$Db_keys[$j-$counters_size-1]."@$r[$j]";
													#print "$PostAppend\n";
													$count1++;
												}
											}
											#print "$PostAppend\n";
											for($i=0;$i<$counters_size;$i++)
											{
												my $temp=@$r[$i];
												$counter_printing= $Counters_List[$i];
												$counter_name = $Counters_List[$i];
												
												
													#print "TABLENAME===$table_name\n";
													#print "$counters_size   ".scalar(@multidyn_db_keys)."\n";
													#print $counter_name.$PostAppend.@$r[$counters_size]."\n";
													if($Counters_epfg{$counter_name."-".$PostAppend."-".@$r[$counters_size]} ne '' and ($temp ne ''))
													{
													if(($Counters_epfg{$counter_name."-".$PostAppend."-".@$r[$counters_size]} eq $temp))
													{
														$pass++;
														#$result .= "<tr><td align=left><font color=FF0000><b>$counter_name.$PostAppend.@$r[$counters_size]</b></font></td></tr>";
													}
													else
													{
														print "$Counters_epfg{$counter_name.$PostAppend.@$r[$counters_size]} eq $temp\n";
														$result .= "<tr><td align=left><font color=FF0000><b>$counter_printing</b></font></td><td align=left><font color=FF0000><b>@$r[$counters_size]</b></font></td><td align=left><font color=FF0000><b>$PostAppend</b></font></td><td align=left><font color=FF0000><b>$moid_printing</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
														$fail++;
													}
													}
																					
											}
										}
										}
									if($table_dyn_V ne '')
									{
										$result .= "<tr><td align=left><font color=black><font size=3px><b>Table Name:</b></font></font></td><td align=left colspan=7><font color=blue><font size=3px><b>$table_dyn_V</font></font></td></tr>";
										
										$result .= "<tr><td align=left><font color=FF00FF><font size=2px><b>CounterName</b></font></font></td><td align=left><font color=FF00FF><font size=2px><b>FlexName</b></font></font></td><td align=left><font color=FF00FF><font size=2px><b>Dyn Key</b></font></font></td><td align=left><font color=FF00FF><font size=2px><b>Moid</b></font></font></td><td align=left><font color=FF00FF><font size=2px><b>Result</b></font></font></td></tr>";
										
										print "$table_dyn_V\n";
										my @multidyn_db_keys;
										my $query_for_dynkeys = "select distinct dataname,dataid from dataitem where dataformatid like '%$package%$table_dyn_V:%' and dataid in ($Dyn_key_array)";
								
										my $result_for_dynkeys = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$query_for_dynkeys,"ALL");
										my @Db_keys;
										for my $r2 (@$result_for_dynkeys){
											my $temp=@$r2[0];
											push(@multidyn_db_keys,",".$temp);
											push(@Db_keys,@$r2[1]);
										}
									
									#------------- getting data from dwhdb ----------#
#=begin
										my $counters_for_query = join(',', @{$hash_for_counter{$table_dyn_V}});
										
										undef $table_name;
										$table_name = $table_dyn_V."_RAW";
									# if($table_name eq 'DC_E_NR_EVENTS_NRCELLCU_FLEX_DYN_RAW')
									# {
										my $queryy = "select $counters_for_query,FLEX_FILTERNAME,DCVECTOR_INDEX @multidyn_db_keys from $table_name where DATETIME_ID = '$datetime' and moid like '$moid' and $Nodenamekey like '$Nodename'";
										
										#print "$queryy\n";
										
										undef $result_from_dwhdb;
										$result_from_dwhdb = executeSQL("dwhdb",2640,"dc",$DCPass,$queryy,"ALL");
										our @Counters_List = @{$Counters_List_array{$table_dyn_V}};
										$counters_size = @{$Counters_List_array{$table_dyn_V}};
										my $k=0;
									#-------------Start of processing the outout data of dwhdb -------------#
									my %Temp_hash_db_vector;
										for my $r (@$result_from_dwhdb)
										{								
											my $col_count = @$r;
											our $counters_size = @Counters_List;
											my $i;
											my $PostAppend = '';
											my $count1=0;
											#print $counters_size ." " . scalar(@Db_keys) ."\n";
											for(my $j=$counters_size+2;$j<$counters_size+scalar(@Db_keys)+2;$j++)
											{
												if(@$r[$j] ne '')
												{
													$PostAppend = $PostAppend.$Db_keys[$j-$counters_size-2]."@$r[$j]";
													#print "$PostAppend\n";
													$count1++;
												}
											}
											#print "$PostAppend\n";
											for($i=0;$i<$counters_size;$i++)
											{
												my $temp=@$r[$i];
												if($Counter_Type{$Counters_List[$i]} eq "FlexdynComvector" and $temp ne '')
												{
													push @{$Temp_hash_db_vector{$Counters_List[$i]."-".$PostAppend."-".@$r[$counters_size]}}, @$r[$counters_size+1];
													push @{$Temp_hash_db_vector{$Counters_List[$i]."-".$PostAppend."-".@$r[$counters_size]}}, $temp;
												}
												elsif($temp ne '')
												{
													push @{$Temp_hash_db_vector{$Counters_List[$i]."-".$PostAppend."-".@$r[$counters_size]}}, $temp;
												}	
												
											}
										}
											for my $Counters (keys %Temp_hash_db_vector)
											{
												my @Split = split("-",$Counters); 
												my $Fun = join(',',@{$Temp_hash_db_vector{$Counters}});
												my $count = @{$Temp_hash_db_vector{$Counters}};
												if($Counter_Type{$Split[0]} eq "FlexdynComvector")
												{
													$Fun = ${$Temp_hash_db_vector{$Counters}}[$count-2]+1 .",$Fun";
												}
												if($Counter_Type{$Split[0]} eq "CMVECTOR")
												{
													my @CMV = split(',',$Counters_epfg{$Counters});
													$Counters_epfg{$Counters} = join(',',@CMV[1..scalar(@CMV)-1]);
												}
													if(($Counters_epfg{$Counters} eq $Fun))
													{
														$pass++;
														#$result .= "<tr><td align=left><font color=FF0000><b>$Counters</b></font></td></tr>";
													}
													else
													{
														print "$Counters_epfg{$Counters} eq $Fun\n";
														$result .= "<tr><td align=left><font color=FF0000><b>$Split[0]</b></font></td><td align=left><font color=FF0000><b>$Split[2]</b></font></td><td align=left><font color=FF0000><b>$Split[1]</b></font></td><td align=left><font color=FF0000><b>$moid_printing</b></font></td><td align=left><font color=FF0000><b>FAIL</b></font></td></tr>";
														$fail++;
													}
													
																					
											}
										
										}
										#} Table if;
										}
										#print "$pass + $fail\n";
										#print scalar(keys %nrml_counters_epfg)."\n";
									#-------------end of processing the outout data of dwhdb -------------#
							}# while read csv file close
						}# if correct_intf_file
					} # while read dir 
				}# open dir 
			} # if tp name
		} #if dir
	} # for of result_from_repdb
	
	return $result;	
}

##############################################################################################################################################

print "********Executing MultiDynCounter_Validation Script********\n";
	
	my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> MultiDyn Counter Data Validation </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	$report.= "<td><b>$stime\t</td>";
	 
	my $result1=Verify_MultiDynCounters();
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>END TIME:\t</td>";
    my $etime = getTime();
	$report.= "<td><b>$etime\t</td>";
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	$report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($pass) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($fail)</td>";
	print "MultiDyn_Counter_Data_Validation:PASS- $pass FAIL- $fail\n";
	$report.= "</table>";
	$report.= "<br>";
	 
	$report.="$result1";
	 
    $report.= getHtmlTail();
    my $file = writeHtml("MultiDynCounter_Validation".$datenew,$report);

print "********MultiDynCounter_Validation Test Case is done********\n";