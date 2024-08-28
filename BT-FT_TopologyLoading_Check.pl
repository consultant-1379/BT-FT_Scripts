#!/usr/bin/perl

use strict;
use warnings;
no warnings 'uninitialized';

use DBI;
use Data::Dumper;
use Text::CSV;

my $topoloading = "true";
my $result;
my $report="";
my $num = 0;
my $num1 = 0;
my $Skip_Num=0;
my $result_Skip;
my $LOGPATH="/eniq/home/dcuser/BT-FT_Log";
my $TopoNodenameKey = $ARGV[0];
my $TopoNodename = $ARGV[1];
my $node_no_data;
my $node_pass;
open(Pass, "<Passwords.txt");
	my $name = do {local $/; <Pass> };
	my ($DCPass,$DwhrepPass,$EtlrepPass) = split("\n",$name);
	#print "DCPass: $DCPass , DwhrepPass: $DwhrepPass , EtlrepPass: $DwhrepPass";

#EQEV-126991 : BT-FT script to be enhanced to handle multiple Node name.
my @input_Toponode_array= split(",",$TopoNodename);
	
my $Toponode_names = join("','",@input_Toponode_array);
#print Dumper(\@input_Toponode_array);
#print "node_names :$Toponode_names\n";

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
print  OUT $out;
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

sub topoloading{
				
				my @array;
				my %tp_name_hash;
				my %out_node_for_logs;
				my ($strings,$r,$b);
				my ($line);
				my $i;
				
				my ($sec,$min,$hour,$mday,$mon,$year,$wday, $yday,$isdst)=localtime(time);
                my $today = sprintf "%4d-%02d-%02d ", $year+1900,$mon+1,$mday;
				#print "$today\n";
				
				
				my @tp_name ="";
				my $file = 'data.txt';
				open FILE, '<', $file or die "Could not open '$file', No such file found in the provided path $!\n";
				@array = <FILE>;
				#print "@array\n";
				my $count = @array;
				#print "$count\n";
				
				for ($i=0;$i<$count;$i++) {
				undef $line;
	            $line = shift @array;
				chomp $line;
				#print "$line\n";
				($strings,$r,$b) = $line  =~ m/(DIM_E_\w*)(\_R.+\_b)(\d+)/;
				my $final_str = $strings.":((".$b."))";
				#print "$final_str\n";
				
				if ($strings=~ m/(DIM_E_\w*)/)
				{
				foreach my $tp ($final_str) {
				
				$result.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$tp" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="100%" >};
				$result .= "<tr><td align=left><font color=black><font size=4px><b>TP NAME:</b></font></font></td><td align=left colspan=3><font color=blue><font size=4px><b>$tp</b></font></font></td></tr>";
				$result .= "<tr><td align=left><font color=000000><b>TABLE NAME</b></font></td><td align=left><font color=000000><b>NODE NAMES</b></font></td><td align=left><font color=000000><b>NO OF ROWS LOADED</b></font></td><td align=left><font color=000000><b>RESULT</b></font></td></tr>";
				$result_Skip.=qq{<body bgcolor=GhostWhite> </body> <center> <table  id="$tp" BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="100%" >};
					
				$result_Skip.= "<tr><td align=left><font size=3px><font color=magenta><b>Skipped ($TopoNodenameKey not Present)</b></font></font></td></tr>";										
				
				#my $tp_query = "select TYPENAME from ReferenceTable where VERSIONID like '$tp' and TYPENAME like 'DIM%'";
				my $tp_query = "select distinct substr(ReferenceColumn.TYPEID,charindex('))',ReferenceColumn.TYPEID)+3) as tab, DATANAME from ReferenceColumn where TYPEID like '$tp%' order by tab;";
				#print "tp_query :$tp_query\n";
				my ($tp_names) = executeSQL("repdb",2641,"dwhrep",$DwhrepPass,$tp_query,"ALL");
				#print Dumper(\$tp_names);
				undef %tp_name_hash;
				
					for my $rows ( @$tp_names ) {
						my ($table, $counter) = @$rows;
				
							push @{$tp_name_hash{$table}}, $counter;
					}
				
				
				foreach (keys %tp_name_hash ){
				my $flag = 0; 
				my $node_names_output='';
				my $counttt='';
				undef %out_node_for_logs;
				my $tab_name = $_;
				$tab_name =~ s/ //g;
				#print "$tab_name\n";
				my $res = $tp_name_hash{$tab_name};
				#print "res :$res\n";
				if(grep(/^$TopoNodenameKey$/,@$res)){
					#my $query2 = "select count(*) from $tab_name where $TopoNodenameKey = '$TopoNodename'" ;
					my $query2 = "select $TopoNodenameKey,count(*) from $tab_name where $TopoNodenameKey in ('$Toponode_names') group by $TopoNodenameKey" ;

					my ($res2) = executeSQL("dwhdb",2640,"dc",$DCPass,$query2,"ALL");
					#print "query2 :$query2\n";
					
					#----------------------------
					for my $m ( @$res2 ) {
						$out_node_for_logs{@$m[0]}=@$m[1];
						$flag = 1;
					}
					foreach my $s (keys %out_node_for_logs){
						$node_names_output .= $s.",";
						$counttt .= $out_node_for_logs{$s}.",";
						#print "$s\n";
					}
					#if ($res2 == '0'){
					chop($node_names_output);
					chop($counttt);
					if($flag == 0){
						$result .= "<tr><td align=left><font color=FF0000><b>$tab_name</b></font></td><td align=left><font color=FF0000><b>$TopoNodename</b></font></td><td align=left><font color=4169E1><b>COUNT:0</b></font></td><td align=left><font color=FF0000><b>DATA NOT LOADED</b></font></td></tr>";
						$num++;
						#print "$num\n";
					}else{
						# ---------------commenting out the code to reduce the logs------------------------------ #
						#$result .= "<tr><td align=left><font color=008000><b>$tab_name</b></font></td><td align=left><font color=008000><b>$node_names_output</b></font></td><td align=left><font color=4169E1><b>COUNT:$counttt</b></font></td><td align=left><font color=008000><b>DATA LOADED</b></font></td></tr>";
						# ---------------commenting out the code to reduce the logs------------------------------ #
						
						$num1++;
						#print "$num1\n";
					}
					#------------------------
				}else{
					$result_Skip .= "<tr><td align=left><font color=000000><b>$tab_name</b></font></td></tr>";
					$Skip_Num++;
				}
				
			}

		}
	}else {
		print "$line: This is not a Topology TP. VERIFY_TOPOLOGY_DATA_LOADING testcase is only for Topology TP.\n";
	}
}
$result.=$result_Skip;
return $result;
}


##############################################################################################################################################

if($topoloading eq "true")
   {
     
	 print "********Executing Topology Data Loading Check Script********\n";

	 my $report =getHtmlHeader();
	$report.="<h1> <font color=MidnightBlue><center> <u> VERIFY TOPOLOGY DATA LOADING </u> </font> </h1>";
	
	$report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	$report.= "<tr>";
	$report.= "<td><font size = 2 ><b>START TIME:\t</td>";
	my $stime = getTime();
	 $report.= "<td><b>$stime\t</td>";
	 
	 my $result1=topoloading();
	    	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>END TIME:\t</td>";
     my $etime = getTime();
	 $report.= "<td><b>$etime\t</td>";
	 
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>RESULT:\t</td>";
	 $report.= "<td><a href=\"#t1\"><font size = 2 color=green><b>PASS ($num1) / <a href=\"#t2\"><font size = 2 color=red><b>FAIL ($num) / <font size = 2 color=Fuchsia><b>Skipped ($Skip_Num)</td>";
	 print "Topology_Loading Check:PASS- $num1 FAIL- $num \n";
	 $report.= "</table>";
	 $report.= "<br>";
	 
	 #$result.="<h2> $result1 </h2>";
	 $report.=qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="80%" >};
	 $report.= "<tr>";
	 $report.= "<td><font size = 2 ><b>$result1\t</td>";
	 $report.= "</table>";
	 
     $report.= getHtmlTail();
     my $file = writeHtml("VERIFY_TOPOLOGY_DATA_LOADING_".$datenew,$report);
	# print "PARTIAL FILE: $file\n"; 
     $topoloading ="false";
	 print "********Topology Data Loading Check Script Test Case is done********\n";
   }