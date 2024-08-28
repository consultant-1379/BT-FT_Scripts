#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;

my $dataloadingmo = "true";
my $result;
my $report="";
my $no_data = 0;
my $data = 0;
my $no_data_deprecated =0;

my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";
open(Pass, "<Passwords.txt");
	my $name = do {local $/; <Pass> };
	my ($DCPass,$DwhrepPass,$EtlrepPass) = split("\n",$name);
	#print "DCPass: $DCPass , DwhrepPass: $DwhrepPass , EtlrepPass: $DwhrepPass";
sub trim
{
    my $str = $_[0];
    $str=~s/^\s+|\s+$//g;
    return $str;
}

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
if ($testCase eq "")
{
return qq{<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
<head>
<title>
ENIQ Regression Feature Test
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
else
{
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

############################################################
# ExecuteSQL
# This will give the resultset data for the queries passed

sub executeSQL{

        my $dbname = $_[0];
        my $port = $_[1];
        my $cre = $_[2];
		my $cre1 = $_[3];
        my $arg = $_[4];
        my $type = $_[5];
        #print "executeSQL : $arg  $type $cre $port $dbname\n\n";
                
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

################################################################################################################
																							
sub uniq {
			return keys  %{{ map { $_ => 1 } @_ }};
	}

############################################################
# Dataloading MO this will verify whether the data is matching with the EPFG input file.
# 
sub dataloadingmo{
	
	my ($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$TopoNodeNameKey);
	my @table_name_array = ();
	my @array = ();
	
	my $file = 'data_new.txt';
	open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
	@array = <FILE>;
	my $line;
	undef $line;
	$line = shift @array;
	chomp $line;	
	#print "$line\n";
	
	($tp_name,$r,$build,$junk,$junk1,$datetime,$junk2,$Nodenamekey,$junk3,$Nodename,$junk4,$TopoNodeNameKey) = $line  =~ m/(DC_\w*)(\_R.+\_b)(\d+)(.\w*)(\|)(\d+\-\d+\-\d+ \d+\:\d+\:\d+)(\|)(\w+)(\:)(.+)(\|)(.+)/;
	#print "$Nodename\n";
	my $package = $tp_name.":((".$build."))";
	
	#EQEV-126991 : BT-FT script to be enhanced to handle multiple Node name.
	my ($year,$hyphen1,$month,$hyphen2,$date,$space,$hour,$colon1,$min,$colon2,$sec) = $datetime =~ m/(\d+)(-)(\d+)(-)(\d+)( )(\d+)(:)(\d+)(:)(\d+)/;
	#my $erbsdatetime = "$year-$month-$date ".(int($hour)+2).":$min:$sec";
	my $erbsdatetime1 = "$year-$month-$date";

	if ($tp_name =~ m/(DC_\w*)/) {
				
		$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$package" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
		$result .= "<tr><td align=left><font color=black><font size=5px><b>TP NAME:</b></font></font></td><td align=left colspan=2><font color=blue><font size=5px><b>$package</b></font></font></td></tr>";
		$result .= "<tr><td align=left><font color=000000><b>TABLE NAME</b></font></td><td align=left><font color=000000><b>Node Name</b></font></td><td align=left><font color=000000><b>Number of Rows</b></font></td><td align=left><font color=000000><b>RESULT</b></font></td></tr>";
		
		my $table_name_query = "select distinct TYPENAME from MeasurementType where TYPEID like '%".$package."%' and TYPENAME not like '%BH'";
		my ($table_names) = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$table_name_query,"ALL");
		
		for my $rows_from_table ( @$table_names ){
			
			for my $field_from_table ( @$rows_from_table ){
			
				if ( $field_from_table !~ m/^dim*/ || $field_from_table !~ m/^DIM*/ ){
				
				push @table_name_array, $field_from_table;
				
				}
			}
		}
		#EQEV-126991 : BT-FT script to be enhanced to handle multiple Node name.
		my @input_node_array= split(",",$Nodename);
		my %node_map;
		
		my $node_names = join("','",@input_node_array);
		#print Dumper(\@input_node_array);
		my $node_query;

		foreach ( @table_name_array ){
			my $flag =0;		
			my $condition_table_name = $_;
			$condition_table_name =~ s/ //g;
			
			my $dwh_query = "select distinct $Nodenamekey ,count(*) from ".$condition_table_name."_raw where $Nodenamekey in ('$node_names') and DATE_ID in ('$erbsdatetime1') group by $Nodenamekey";
			###print "dwh_query :$dwh_query\n";
            my ($result_dwh) = executeSQL("dwhdb",2640,"dc",$DCPass,$dwh_query,"ALL");
			
			my $node_names_output;
			my $counttt;
			my $node_length = @input_node_array;
			
			foreach my $str (@input_node_array){
				$node_map{$str} =0;
			}
			
				if(defined $result_dwh){
					foreach my $m (@$result_dwh){
		
						$node_map{@$m[0]} = @$m[1];
						$flag = 1;
					}
				}
				foreach my $s (keys %node_map){
					$node_names_output .= $s.",";
					$counttt .= $node_map{$s}.",";
				}

			chop($node_names_output);
			chop($counttt);
			#print "node_names_out :$node_names_output\n";
			
			if($flag == 0){
				
				my $rep_query = "select followjohn from MeasurementType where typename like '$condition_table_name' and TYPEID like '$package:%';" ;
				my ($result_repdb_1)= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$rep_query,"ALL");
				
				for my $r1 (@$result_repdb_1){
					my $temp1=@$r1[0];
				
					if($temp1 == "0"){
						#print "temp1 :$temp1\n";
						$result .= "<tr><td align=left><font color=DarkOrange><b>$condition_table_name</b></font></td><td align=left><font color=DarkOrange><b>$node_names_output</b></font></td><td align=left><font color=DarkOrange><b>$counttt</b></font></td><td align=left><font color=DarkOrange><b>Deprecated</b></font></td></tr>";
						$no_data_deprecated++;
					}else{
						my $rep_query_2 = "select description from MeasurementType where typename like '$condition_table_name' and TYPEID like '$package:%';" ;
						my ($result_repdb_2)= executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$rep_query_2,"ALL");
				
				
						for my $r2 (@$result_repdb_2){
							my $temp=@$r2[0];
							
							my $Pattern_flag=0;
							my @desc_patterns   = ("Deprecated:(To be deprecated in n-8 release)","Deprecated since", "Deprecated,since", "Deprecated,Since", "Deprecated Since", "Deprecated. Since", "Deprecated. since", "Deprecated only in", "Deprecated in", "Deprecated: Since", "Deprecated:Since", "Deprecated.", "Deprecated,");
										
							for my $pattern (@desc_patterns) {
								#print "pattern :$temp\n";
								if($temp =~ m/\w*$pattern\w*/){
									$Pattern_flag=1;
									last;
								}
							}
							if($Pattern_flag == 1){
											
								$result .= "<tr><td align=left><font color=DarkOrange><b>$condition_table_name</b></font></td><td align=left><font color=DarkOrange><b>$node_names_output</b></font></td><td align=left><font color=DarkOrange><b>$counttt</b></font></td><td align=left><font color=DarkOrange><b>Deprecated</b></font></td></tr>";
								$no_data_deprecated++;
							}else{
								$result .= "<tr><td align=left><font color=FF0000><b>$condition_table_name</b></font></td><td align=left><font color=FF0000><b>$node_names_output</b></font></td><td align=left><font color=FF0000><b>$counttt</b></font></td><td align=left><font color=FF0000><b>DATA NOT LOADED</b></font></td></tr>";
								$no_data++;
							}
						}
					}
				}
			}
			else{
			
				$result .= "<tr><td align=left><font color=008000><b>$condition_table_name</b></font></td><td align=left><font color=008000><b>$node_names_output</b></font></td><td align=left><font color=4169E1><b>$counttt</b></font></td><td align=left><font color=008000><b>DATA LOADED</b></font></td></tr>";
				$data++;
			
			}
		}
		$result.= "</table>";
		$result.= "<br>";
	}
	else {
		print "$tp_name: This is not a PM TP. This testcase is only for PM TP.\n\n\n";
	}
	return $result;
}
##############################################################################################################################################

if($dataloadingmo eq "true")
   {
     print "********Executing Data Loading Check Script********\n";

	 
	 my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> VERIFY DATA LOADING </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	 $report.= "<td><b>$stime\t</td>";
	 
	 my $result1=dataloadingmo();
	    	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
     my $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($data) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($no_data) / <a href=\"#t2\"><font size = 2 color=DarkOrange><b>DEPRECATED ($no_data_deprecated)</td>";
	 print "Data_Loading Check:PASS- $data FAIL- $no_data\n";
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 #$result.="<h2> $result1 </h2>";
	 $report.="$result1";
	 
     $report.= getHtmlTail();
     my $file = writeHtml("VERIFY_TABLE_DATA_LOADING_".$datenew,$report);
	 #print "PARTIAL FILE: $file\n"; 
     $dataloadingmo ="false";
	 
	 print "********Data Loading Check Script Test Case is done********\n";
   }


