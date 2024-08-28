#!/usr/bin/perl -C0

use strict;
use warnings;
use Text::CSV;
use File::Slurp;
use POSIX qw(strftime);
use Data::Dumper;
use MCE::Grep Sereal => 1;
use MCE::Loop;
use MCE::Util;

my $LOGPATH = "/eniq/home/dcuser/BT-FT_Log";
my $CUT = "/usr/bin/cut";

#######################################################

my $contents = qq{<br><table border="1">
				<tr>
				<th colspan="3"><font size="2" color=Blue><b><u>Contents</u></b></font></th>
				</tr>
				<tr>
				<th>No.</th>
				<th>Test Case</th>
				<th>Result</th>
				</tr>
				};
my $con_index = 0;

##################################################################################
#             The DATETIME value for the FILENAME of the HTML LOGS               #
##################################################################################

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
$mon++;
$year = 1900 + $year;
my $datenew = sprintf("%4d%02d%02d%02d%02d%02d",$year,$mon,$mday,$hour,$min,$wday);

################################################REWRITE########################################################

###################################################################
# getEndTimeHeader_Overall
# this subroutine returns the end time of each overall result page
# in a standard format
sub getEndTimeHeader_Overall{
	my $pass = shift;
	my $fail = shift;
	my $rep .= "<tr>";
	$rep .= qq{<tr>
		<th> <font size = 2 > END TIME </th>
		<td><font size = 2 ><b>};
	my $etime = getTime();
	$rep .= "$etime";
	$rep .= "<tr>";
	$rep .=qq{<tr>
		<th> <font size = 2 > RESULT SUMMARY </th>
		<td><font size = 2 ><b>};
	$rep .= "<a href=\"#t1\">PASS ($pass) / <a href=\"#t2\">FAIL ($fail)";
	print "Log Check:PASS- $pass FAIL- $fail\n"; 
	$rep .= "</table>";
	$rep .= "<br>";
	return $rep;
}

############################################################
# WRITE HTML
# This is a utility for the log output file in HTML 
sub writeHtml{
	my $server = shift;
	my $out = shift;
	open(OUT," > $LOGPATH/$server.html");
	print OUT $out;
	close(OUT);
	return "$LOGPATH/$server.html\n";
}

###################################################################
# getEndTimeHeader_Log
# this subroutine returns the end time of each overall result page
# in a standard format
sub getEndTimeHeader_Log
{
	my $rep .= "<tr>";
	$rep .= qq{<tr>
		<th> <font size = 2 > END TIME </th>
		<td><font size = 2 ><b>};
	my $etime = getTime();
	$rep .= "$etime";
	$rep .= "<tr>";
	$rep .= "</table>";
	$rep .= "<br>";
	return $rep;
}

sub checkLogs {
	my @results = ();
	my $result ="";
	my @files = @{$_[0]};
	my @logfilters = undef;
	@logfilters = @{$_[1]};
	my $filterList = join('|',@logfilters);
	open(FH,"<readfile.txt") or die "Couldn't open file file.txt, $!";;
	my $string = do {local $/; <FH> };
	close (FH); 
	my @ignoreLogFilters = split(',',$string);
	my $ignoreList = join('|',@ignoreLogFilters);
	my @errData = undef;
	my $cnt =0;
	for my $file (@files){
		next if($file eq "");
		#print "\nNew File : $file\n";
		if (@logfilters ne undef){
			@errData = mce_grep_f { /$filterList/i && not /$ignoreList/i } $file;
		}
		else {
			@errData = mce_grep_f { not /$ignoreList/i } $file;
			my $cnt1 = $#errData + 1;
			#print "Type 2 : $cnt1\n";
		}
		chomp(@errData);
		$cnt = $#errData + 1;
		#print "Matched Lines : $cnt\n";
		if (@errData != 0) {
			$result .= "<h3><b>FILE : $file</b></h3>";
			for my $line (@errData) {
				$_ = $line;
                if(/java.lang.|ASA Error|SEVERE|reactivated/) {
                    $result .= "<font color=660000><b>$line</b></font><br>";
                }   
				else {
                    $result .= "$line<br>";
                }
			}
		}
		#print "Done for $file\n";
	}
	push(@results,$result,$cnt);
	return @results; 
}

sub verifyLogs{
	my @results=();
	my $result = "";
	my $tp_name = "";
	my $failcount=0;
	my $file = '/eniq/home/dcuser/BT-FT_Script/data.txt';
	open my $fh, '<', $file or die "Could not open '$file' $!\n";
	while (my $line = <$fh>) {
		chomp $line;
		my ($strings,$r,$b) = $line  =~ m/(\w*_\w*)(\_R.+\_b)(\d+)/;
		foreach ($strings) {
			$tp_name = $strings;
			#print "$tp_name\n";
			my @enginelogFilters=("error","exception","fatal","severe","warning","not found","cannot","not supported","reactivated","failed");
			my @svclogFilters=("error","exception","fatal","severe","warning","not found","cannot","not supported","reactivated","Unknown Source","NoClassDefFoundError","failed"); 
			my @iqmsgLogFilters=("Dump all thread stacks at","Abort","fatal","Error","Please report this to SAP IQ support","^E.","failed");

			my %basedirList;
			if ( $^O ne "linux" ) {
				$basedirList{'/var/svc/log'} = [ @svclogFilters ];
			}
			#$basedirList{'/eniq/local_logs/iq'} = [ @iqmsgLogFilters ];
			$basedirList{'/eniq/log/sw_log/tp_installer'} = [ @enginelogFilters ];
			$basedirList{"/eniq/log/sw_log/engine/".$tp_name} = [ @enginelogFilters ];

			my @filters;
			my @fileList;
			for my $dirPath (keys %basedirList) {
				if ( $dirPath eq '/eniq/local_logs/iq' ) {
					@fileList = glob "$dirPath/*.*";
				}
				else {
					@fileList = glob "$dirPath/*.log";
				}
				@fileList = grep { -M $_ < 1 } @fileList;
				chomp(@fileList);
				#print "File List : @fileList\n";
				if (exists $basedirList{$dirPath}) {
					@filters = @{$basedirList{$dirPath}};
					$result .= "<h3><b>PATH : $dirPath</b></h3>";
					#print "PATH : $dirPath\n";
					@results = checkLogs(\@fileList,\@filters);
					$result.=$results[0];
					$failcount=$failcount+$results[1];
				}
				my @subDirs = read_dir( $dirPath, prefix => 1 );
				for my $subDir (@subDirs){
					if ( (-d $subDir) && (index($subDir, '.') eq -1) ) {
						$result .= "<h3><b>PATH : $subDir</b></h3>";
						@fileList = glob "$subDir/*.log";
						@fileList = grep { -M $_ < 1 } @fileList;
						chomp(@fileList);
						#print "SubDir : $subDir   File List : @fileList\n";
						for my $key (keys %basedirList) {
							if (index($subDir, $key) ne -1) {
								@filters = @{$basedirList{$key}};
								last;
							}
						}
						@results = checkLogs(\@fileList,\@filters);
						$result.=$results[0];
						$failcount=$failcount+$results[1];
					}
					if ( $subDir eq '/eniq/log/sw_log/engine' ) {
						my @sub = read_dir( $subDir, prefix => 1 );
						for my $dir (@sub) {
							if ( (-d $dir) && (index($dir, '.') eq -1) ) {
								$result .= "<h3><b>PATH : $dir</b></h3>";
								@fileList = glob "$dir/*.log";
								@fileList = grep { -M $_ < 1 } @fileList;
								chomp(@fileList);
								#print "SubDir : $dir   File List : @fileList\n";
								for my $key (keys %basedirList) {
									if (index($dir, $key) ne -1) {
										@filters = @{$basedirList{$key}};
										last;
									}
								}
								@results = checkLogs(\@fileList,\@filters);
								$result.=$results[0];
								$failcount=$failcount+$results[1];
							}
						}
					}
				}
				$result .= "<br>\n";
			}
		}
	}
	@results=();
	push(@results,$result,$failcount);
	return @results; 
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
# getEndTimeHeader
# this subroutine returns the end time of each test case
# in a standard format
sub getEndTimeHeader{
	my $pass = shift;
	my $fail = shift;
	my $rep .= "<tr>";
	$rep .= qq{<tr>
		<th> <font size = 2 > END TIME </th>
		<td><font size = 2 ><b>};
	my $etime = getTime();
	my $server = getHostName();
	$rep .= "$etime";
	$rep .= "<tr>";
	$rep .=qq{<tr>
		<th> <font size = 2 > RESULT SUMMARY </th>
		<td><font size = 2 ><b>};
	$rep .= "<a href=\"#t1\">PASS ($pass) / <a href=\"#t2\">FAIL ($fail)";
	$rep .= qq{<tr>
		<th> <font size = 2 > DETAILED RESULT </th>
		<td><font size = 2 ><b>};
	$rep .= "<a href=\"$server\_$datenew.html\" target=\"_blank\">Click here</a>";
	$rep .= "</table>";
	$rep .= "<br>";
	$rep .= "<h3><font size=4 color=\"Blue\"><b><u>Note:</u> Only Failed TestCases shown, refer link above for Detailed Results</b></font></h3><br>";
	return $rep;
}

############################################################
############################################################
# UTILITY TO EXECUTE ANY COMMAND AND GET RESULT IN ARRAY
sub executeThis{
	my $command = shift;
	my @res = `$command`; 
	return @res;
}
############################################################
# GET DATE
# This is a utility
sub getDate{
	my ($sec,$min,$hour,$mday,$mon,$year,$wday, $yday,$isdst)=localtime(time);
	return sprintf "%4d%02d%02d%02d%02d%02d", $year+1900,$mon+1,$mday,$hour,$min,$sec;
}

############################################################
# TRIGGER TESTCASES
# This sub-routine will execute each 
# testcase and return result
sub parseParam{
	my @results=();
	my $result = "";
	my $DATE = getDate();
	my $baselinePath = "";
	my $featureBaselinePath = "";
	
	{
		$con_index++;
		$contents .=	qq{<tr>
			<td>$con_index</td>
			<td><a href="#READLOG_$con_index">LOG VERIFICATION</a></td>
			};
		my $report = getStartTimeHeader("verifyLogs");
		$result .= "<h2><font color=Black><a name=\"READLOG_$con_index\">$DATE LOG VERIFICATION</a></font></h2><br>";
		#print $DATE;
		#print " LOG VERIFICATION\n";
		@results = verifyLogs();
		$result .= $results[0];
		$contents .= qq{<td><a href="#READLOG_$con_index">Verify Logs</a></td>
			</tr>
			};
		$report .= getEndTimeHeader_Log();
		$report .= $results[0];
		$report.= getHtmlTail(); 
		#my $file = writeHtml("Log_Verification",$report);
		#print "PARTIAL FILE: $file\n";
		MCE::Grep::finish;
	}
	$contents .= "</table><br>";
	my $fails = $results[1];
	@results=();
	push(@results,$result,$fails);
	return @results;
}

############################################################
# GET TIMESTAMP
# This is a utility 
sub getTime{
	my ($sec,$min,$hour,$mday,$mon,$year,$wday, $yday,$isdst) = localtime(time);
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

#############################################################
# getStartTimeHeader
# this subroutine returns the start time of each test case
# in a standard format
sub getStartTimeHeader
{
	my $testCase = shift;
	my %testCaseHeading = (
		"verifyLogs","Log Verification",
		"OVERALL","ENIQ Regression Feature Test",
		"compareBaseline","COMPAREBASELINE VERIFICATION",
		);
	my $testCaseHead = $testCaseHeading{$testCase};
	my $rep .= getHtmlHeader($testCaseHead);
	$rep .= "<h1> <font color=MidnightBlue><center> <u> $testCaseHead </u> </font> </h1>";
	$rep .= qq{<body bgcolor=GhostWhite> </body> <center> <table  BORDER="3" CELLSPACING="2" CELLPADDING="3" WIDTH="50%" >
					<tr>
					<th> <font size = 2 >START TIME </th>
					<td> <font size = 2 > <b>};
	my $stime = getTime();
	$rep .= "$stime";
	return $rep;
}

#####################################################################################
sub getdatetime{
	my $yesterdaysTime = strftime "%Y%m%d-%H:%M:%S",localtime(time() - 24*60*60);
	my $hour = `echo $yesterdaysTime -s| $CUT -c 10,11`;
	chomp($hour);
	my $day = `echo $yesterdaysTime -s| $CUT -c 7,8`;
	chomp($day);
	my $month = `echo $yesterdaysTime -s| $CUT -c 5,6`;
	chomp($month);
	my $year = `echo $yesterdaysTime -s| $CUT -c 1-4`;
	chomp($year);
	return sprintf "%02d-%02d-%4d",$day,$month,$year;
}

############################################################
# MAIN
# This is a simple main that starts the generation of the HTML log file and 
# calls the parseParam subroutine that controls the execution of the script
# Then when all tests are finished writes the log HTML file in the same directory 
# where this script is executed
{
	print "********Executing Log Checking Script********\n";
	my $contents = "";
	my $d = getdatetime();

	mkdir("$LOGPATH");
	my $report = getStartTimeHeader("OVERALL");
	$report .= qq{<tr>
		<th> <font size = 2 > HOST </th>
		<td><font size = 2 ><b>};
	my @results=parseParam();
	my $tot_report .= $results[0];
	my $pass=0;
	my $fail=$results[1];	
	#my $fail =()= $tot_report =~ /_FAIL_+/g;
	#my $pass =()= $tot_report =~ /_PASS_+/g;
	#$tot_report =~s/_PASS_/PASS/g;
	#$tot_report =~s/_FAIL_/FAIL/g;
	$report.= getEndTimeHeader_Overall($pass,$fail);
	print "$contents";
	$report .= $contents;
	$report.= $tot_report;
  	$report.= getHtmlTail(); 
  	my $file = writeHtml("LOG_VERIFICATION_".$datenew,$report);

	print "********Log Checking Script Test Case is done********\n";
}

###############################################################################################################################################################

