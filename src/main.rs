use rusqlite::{Connection, Result, types::Value, Statement, Rows, types::Null};
use std::env;
use chrono::{prelude::*, offset::MappedLocalTime, Utc, DateTime};

/*
Could find no valid path from Publish_Packages to source_table to buildinfo_table to checksum_table.

tested on 0ad

could only find a valid path for 

path found for seabios version 1.14.0-2 with hashes:
md5: 156bc65c0acd5087adb08cb7c8b72d1c
sha: 060407081c20a244fa8924a4d7f475c9a644c3479769e2265c75588691f6876b

path found for fonts-sawarabi-gothic version 20161015-4 with hashes:
md5: 9c3b106400fc2948dfd21bd2d6e009d4
sha: 2fe2add38a94ba26a86f18727583821201f8f46691ebb1c01e85b9f2b236cb96

path found for vgabios version 0.7b+ds-1 with hashes:
md5: 547b5b09dbfd1095407d2599108c636e
sha: 91bf124052f65d14497349d188fa7a0af6401703a8b36cad2fc318c9ecfad741

and a few others.

most paths did not exist
*/

/*
0ad

published packages
(1, '0ad', 'amd64', '0.0.17-1', 'jessie', 'games', '2862930', 'pool/main/0/0ad/0ad_0.0.17-1_amd64.deb', 'main', '20161231T000000Z', '8b679b5afa15afc1de5b2faee1892faa', 'd850ad98b399016b3456dd516d2e114fd72c956aa7b5ddaa0858f792bb005c5e', ''), (302884, '0ad', 'amd64', '0.0.21-2', 'stretch', 'games', '5229686', 'pool/main/0/0ad/0ad_0.0.21-2_amd64.deb', 'main', '20170629T000000Z', 'd3a3f59190a550f5347c8fc152706032', 'beac2de1e7ae8d8deaa79c1eda14798dd0d597c0852c9eed7d1486bd23053174', ''), (1812078, '0ad', 'amd64', '0.0.23.1-2', 'buster', 'games', '5471488', 'pool/main/0/0ad/0ad_0.0.23.1-2_amd64.deb', 'main', '20190719T000000Z', '4e9a3f5ddaba8bfc5c41980706f3910e', 'fb5c3af6d1d7cd7f1d3e8d2e04c1b8605b8c38f85e5c285a21c56eaf72f2bb47', ''), (3985057, '0ad', 'amd64', '0.0.23.1-5+b1', 'bullseye', 'games', '5588508', 'pool/main/0/0ad/0ad_0.0.23.1-5+b1_amd64.deb', 'main', '20210906T000000Z', '35412374733ae00cbbc7260596e1d78c', '610e9f9c41be18af516dd64a6dc1316dbfe1bb8989c52bafa556de9e381d3e29', ''), (10389610, '0ad', 'i386', '0.0.17-1', 'jessie', 'games', '2838756', 'pool/main/0/0ad/0ad_0.0.17-1_i386.deb', 'main', '20161231T000000Z', '47f9f448490623194647262d2a0f6243', '74567f2bdbc82bde4c6df2b122faa6cd990b685c32c9ac7665c3ee99c5d0022f', ''), (10692650, '0ad', 'i386', '0.0.21-2', 'stretch', 'games', '5444908', 'pool/main/0/0ad/0ad_0.0.21-2_i386.deb', 'main', '20170629T000000Z', 'a2699f935e63c75fb62e5eb2b52ff9e2', '69b465595c4ee1622ca791c53e1fbed774ea7c83f307b8fc1f56b1befb1b6c07', ''), (12196110, '0ad', 'i386', '0.0.23.1-2', 'buster', 'games', '5742824', 'pool/main/0/0ad/0ad_0.0.23.1-2_i386.deb', 'main', '20190719T000000Z', '6d8e2ffd34cc3ecbd3393b990dfe08ed', '1834ef51185a72824a5869b913844d74b0286e1d2c7b20dbe7dc917fb0530476', ''), (14133154, '0ad', 'i386', '0.0.23.1-5+b1', 'bullseye', 'games', '5875076', 'pool/main/0/0ad/0ad_0.0.23.1-5+b1_i386.deb', 'main', '20210906T000000Z', '6aa7ddbead91129a020d66b14bb2057c', '5deb702574a7421f08d57094d1b53f25eb5d997d133edf27d4b5a338a420c05c', '')]

source table
[(16378, '0ad', '0.0.23.1-5+b1'), (29351, '0ad', '0.0.23.1-4+b1'), (34161, '0ad', '0.0.23.1-5'), (69148, '0ad', '0.0.22-4'), (71828, '0ad', '0.0.22-4+b1'), (93756, '0ad', '0.0.23-1+b1'), (96897, '0ad', '0.0.23-1~bpo9+1'), (105942, '0ad', '0.0.23-1'), (112041, '0ad', '0.0.22-4+b2'), (112792, '0ad', '0.0.22-4+b3'), (117954, '0ad', '0.0.23-1+b2'), (133032, '0ad', '0.0.25b-2+b1'), (138185, '0ad', '0.0.25b-2'), (153972, '0ad', '0.0.25b-2+b2'), (189739, '0ad', '0.0.25b-1.1'), (193007, '0ad', '0.0.25b-1+b1'), (196758, '0ad', '0.0.24b-1'), (204129, '0ad', '0.0.25b-1'), (218910, '0ad', '0.0.25b-1~bpo11+1'), (254255, '0ad', '0.0.22-1'), (257191, '0ad', '0.0.22-2'), (259757, '0ad', '0.0.22-3.1'), (260743, '0ad', '0.0.22-3'), (269932, '0ad', '0.0.23.1-2'), (271472, '0ad', '0.0.23.1-1'), (292253, '0ad', '0.0.23.1-3'), (292415, '0ad', '0.0.23.1-4')]

buildinfo
[(196180, 34161, 'amd64', '0ad', 'Debian', 'amd64', '2020-08-18T11:20:50+00:00', '/build/0ad-zpOmLM/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1597744080"'), (196261, 34161, 'i386', '0ad', 'Debian', 'i386', '2020-08-18T11:24:19+00:00', '/build/0ad-n0U1hB/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1597744080"'), (196732, 34161, 'armhf', '0ad', 'Debian', 'armhf', '2020-08-18T12:43:16+00:00', '/build/0ad-e8gy6x/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1597744080"'), (403765, 69148, 'amd64 source', '0ad', 'Debian', 'amd64', '2018-01-10T16:28:48+00:00', '/build/0ad-0.0.22', '\n CFLAGS=" -Wall -g -O2 -Wextra -pipe -funsigned-char -fstrict-aliasing -Wchar-subscripts -Wundef -Wshadow -Wcast-align -Wwrite-strings -Wunused -Wuninitialized -Wpointer-arith -Wredundant-decls -Winline -Wformat -Wformat-security -Wswitch-enum -Winit-self -Wmissing-include-dirs -Wempty-body -fdiagnostics-color=auto -Wmissing-prototypes -Wstrict-prototypes -Wold-style-definition -Wbad-function-cast -Wnested-externs  -Wmissing-declarations"\n CXXFLAGS=" -Wall -g -O2 -Wextra -pipe -funsigned-char -fstrict-aliasing -Wchar-subscripts -Wundef -Wshadow -Wcast-align -Wwrite-strings -Wunused -Wuninitialized -Wpointer-arith -Wredundant-decls -Winline -Wformat -Wformat-security -Wswitch-enum -Winit-self -Wmissing-include-dirs -Wempty-body -fdiagnostics-color=auto"\n DEB_BUILD_OPTIONS="parallel=5"\n LANG="C"\n LC_ALL="C"\n MAKEFLAGS="-j5"\n SOURCE_DATE_EPOCH="1515598036"'), (403910, 69148, 'i386', '0ad', 'Debian', 'i386', '2018-01-10T17:27:50+00:00', '/build/0ad-oFUcwp/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1515598036"'), (406631, 69148, 'armhf', '0ad', 'Debian', 'armhf', '2018-01-11T02:15:11+00:00', '/build/0ad-ggZJ0p/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1515598036"'), (595670, 96897, 'i386', '0ad', 'Debian', 'i386', '2018-06-07T15:37:36+00:00', '/build/0ad-NV6p17/0ad-0.0.23', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1527577625"'), (595982, 96897, 'armhf', '0ad', 'Debian', 'armhf', '2018-06-07T18:21:51+00:00', '/build/0ad-qoVp5I/0ad-0.0.23', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1527577625"'), (654839, 105942, 'kfreebsd-amd64', '0ad', 'Debian', 'kfreebsd-amd64', '2018-10-17T12:01:27+00:00', '/build/0ad-DPsBc1/0ad-0.0.23', '\n DEB_BUILD_OPTIONS="parallel=2"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1526971110"'), (666967, 105942, 'kfreebsd-i386', '0ad', 'Debian', 'kfreebsd-i386', '2018-10-14T16:24:23+00:00', '/build/0ad-A1QTdd/0ad-0.0.23', '\n DEB_BUILD_OPTIONS="parallel=2"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1526971110"'), (698801, 105942, 'i386', '0ad', 'Debian', 'i386', '2018-05-23T10:38:52+00:00', '/build/0ad-mnkXJV/0ad-0.0.23', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1526971110"'), (699139, 105942, 'armhf', '0ad', 'Debian', 'armhf', '2018-05-23T11:46:51+00:00', '/build/0ad-Bfvcdw/0ad-0.0.23', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1526971110"'), (699689, 105942, 'amd64', '0ad', 'Debian', 'amd64', '2018-05-23T10:47:21+00:00', '/build/0ad-tQlTaF/0ad-0.0.23', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1526971110"'), (906194, 138185, 'amd64', '0ad', 'Debian', 'amd64', '2022-03-28T16:47:57+00:00', '/build/0ad-qL36tQ/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1648466962"'), (906905, 138185, 'armhf', '0ad', 'Debian', 'armhf', '2022-03-28T17:58:30+00:00', '/build/0ad-yxRm0w/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1648466962"'), (907069, 138185, 'arm64', '0ad', 'Debian', 'arm64', '2022-03-28T19:25:41+00:00', '/build/0ad-EzGQcN/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=8"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1648466962"'), (907654, 138185, 'i386', '0ad', 'Debian', 'i386', '2022-03-28T16:02:13+00:00', '/build/0ad-5PpxVM/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LANG="C.UTF-8"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1648466962"'), (1216400, 189739, 'amd64', '0ad', 'Debian', 'amd64', '2021-12-23T03:52:13+00:00', '/build/0ad-sQic0M/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1640194452"'), (1216457, 189739, 'armhf', '0ad', 'Debian', 'armhf', '2021-12-23T04:01:24+00:00', '/build/0ad-lsgorM/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=8"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1640194452"'), (1216461, 189739, 'i386', '0ad', 'Debian', 'i386', '2021-12-23T03:59:24+00:00', '/build/0ad-oEE11N/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1640194452"'), (1216527, 189739, 'amd64 source', '0ad', 'Debian', 'amd64', '2021-12-22T17:46:49+00:00', '/build/0ad-OgC3oD/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=12"\n LANG="en_US.UTF-8"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1640194452"'), (1216608, 189739, 'arm64', '0ad', 'Debian', 'arm64', '2021-12-23T04:54:59+00:00', '/build/0ad-7WtlHS/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1640194452"'), (1257784, 196758, 'amd64', '0ad', 'Debian', 'amd64', '2021-03-07T12:19:00+00:00', '/build/0ad-0MkApz/0ad-0.0.24b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1615110797"'), (1257873, 196758, 'amd64 source', '0ad', 'Debian', 'amd64', '2021-03-07T10:13:33+00:00', '/build/0ad-0.0.24b', '\n CFLAGS=" -Wall -g -O2 -Wextra -pipe -funsigned-char -fstrict-aliasing -Wchar-subscripts -Wundef -Wshadow -Wcast-align -Wwrite-strings -Wunused -Wuninitialized -Wpointer-arith -Wredundant-decls -Winline -Wformat -Wformat-security -Wswitch-enum -Winit-self -Wmissing-include-dirs -Wempty-body -fdiagnostics-color=auto -Wmissing-prototypes -Wstrict-prototypes -Wold-style-definition -Wbad-function-cast -Wnested-externs  -Wmissing-declarations"\n CXXFLAGS=" -Wall -g -O2 -Wextra -pipe -funsigned-char -fstrict-aliasing -Wchar-subscripts -Wundef -Wshadow -Wcast-align -Wwrite-strings -Wunused -Wuninitialized -Wpointer-arith -Wredundant-decls -Winline -Wformat -Wformat-security -Wswitch-enum -Winit-self -Wmissing-include-dirs -Wempty-body -fdiagnostics-color=auto"\n DEB_BUILD_OPTIONS="parallel=5"\n LANG="C"\n LC_ALL="C"\n MAKEFLAGS="-j5"\n SOURCE_DATE_EPOCH="1615110797"'), (1257950, 196758, 'arm64', '0ad', 'Debian', 'arm64', '2021-03-07T12:09:19+00:00', '/build/0ad-DwYYwu/0ad-0.0.24b', '\n DEB_BUILD_OPTIONS="parallel=8"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1615110797"'), (1257995, 196758, 'i386', '0ad', 'Debian', 'i386', '2021-03-07T12:06:49+00:00', '/build/0ad-DnI4BS/0ad-0.0.24b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LANG="C.UTF-8"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1615110797"'), (1258010, 196758, 'armhf', '0ad', 'Debian', 'armhf', '2021-03-07T14:19:05+00:00', '/build/0ad-Lavwpn/0ad-0.0.24b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1615110797"'), (1303463, 204129, 'amd64', '0ad', 'Debian', 'amd64', '2021-08-27T16:14:03+00:00', '/build/0ad-R2iFjP/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1630070910"'), (1303686, 204129, 'amd64 source', '0ad', 'Debian', 'amd64', '2021-08-27T14:03:32+00:00', '/build/0ad-0.0.25b', '\n CFLAGS=" -Wall -g -O2 -Wextra -pipe -funsigned-char -fstrict-aliasing -Wchar-subscripts -Wundef -Wshadow -Wcast-align -Wwrite-strings -Wunused -Wuninitialized -Wpointer-arith -Wredundant-decls -Winline -Wformat -Wformat-security -Wswitch-enum -Winit-self -Wmissing-include-dirs -Wempty-body -fdiagnostics-color=auto -Wmissing-prototypes -Wstrict-prototypes -Wold-style-definition -Wbad-function-cast -Wnested-externs  -Wmissing-declarations"\n CXXFLAGS=" -Wall -g -O2 -Wextra -pipe -funsigned-char -fstrict-aliasing -Wchar-subscripts -Wundef -Wshadow -Wcast-align -Wwrite-strings -Wunused -Wuninitialized -Wpointer-arith -Wredundant-decls -Winline -Wformat -Wformat-security -Wswitch-enum -Winit-self -Wmissing-include-dirs -Wempty-body -fdiagnostics-color=auto"\n DEB_BUILD_OPTIONS="parallel=13"\n LANG="C"\n LC_ALL="C"\n MAKEFLAGS="-j13"\n SOURCE_DATE_EPOCH="1630070910"'), (1303959, 204129, 'i386', '0ad', 'Debian', 'i386', '2021-08-27T16:12:21+00:00', '/build/0ad-Fs4cwu/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LANG="C.UTF-8"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1630070910"'), (1303980, 204129, 'armhf', '0ad', 'Debian', 'armhf', '2021-08-27T16:02:54+00:00', '/build/0ad-kz5SfA/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=8"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1630070910"'), (1304047, 204129, 'arm64', '0ad', 'Debian', 'arm64', '2021-08-27T16:21:03+00:00', '/build/0ad-V3WOsS/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=8"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1630070910"'), (1390098, 218910, 'armhf', '0ad', 'Debian', 'armhf', '2021-11-22T16:20:11+00:00', '/build/0ad-rfCSQA/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=8"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1637367405"'), (1390126, 218910, 'i386', '0ad', 'Debian', 'i386', '2021-11-22T15:51:34+00:00', '/build/0ad-B7jwgi/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1637367405"'), (1390538, 218910, 'amd64 source', '0ad', 'Debian', 'amd64', '2021-11-20T02:40:37+00:00', '/build/0ad-0ibmPS/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=1"\n LANG="C.UTF-8"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n MAKEFLAGS=" -j1"\n SOURCE_DATE_EPOCH="1637367405"'), (1391194, 218910, 'arm64', '0ad', 'Debian', 'arm64', '2021-11-22T15:58:55+00:00', '/build/0ad-CqpAbF/0ad-0.0.25b', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n LC_COLLATE="C.UTF-8"\n SOURCE_DATE_EPOCH="1637367405"'), (1680369, 254255, 'amd64 source', '0ad', 'Debian', 'amd64', '2017-10-18T17:14:10+00:00', '/build/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LANG="C"\n LC_ALL="C"\n SOURCE_DATE_EPOCH="1508333597"'), (1680581, 254255, 'i386', '0ad', 'Debian', 'i386', '2017-10-18T19:09:53+00:00', '/build/0ad-suKM8z/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1508333597"'), (1703355, 257191, 'i386', '0ad', 'Debian', 'i386', '2017-10-27T23:09:17+00:00', '/build/0ad-40Q8L4/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1509114135"'), (1703513, 257191, 'amd64 source', '0ad', 'Debian', 'amd64', '2017-10-27T21:49:55+00:00', '/build/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LANG="C"\n LC_ALL="C"\n SOURCE_DATE_EPOCH="1509114135"'), (1722924, 259757, 'i386', '0ad', 'Debian', 'i386', '2017-11-21T01:34:09+00:00', '/build/0ad-iOXEFO/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1511223310"'), (1723914, 259757, 'armhf', '0ad', 'Debian', 'armhf', '2017-11-21T04:23:26+00:00', '/build/0ad-YAxgdn/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1511223310"'), (1723930, 259757, 'amd64', '0ad', 'Debian', 'amd64', '2017-11-21T03:33:05+00:00', '/build/0ad-0UKVKQ/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1511223310"'), (1729389, 260743, 'amd64 source', '0ad', 'Debian', 'amd64', '2017-11-04T12:29:10+00:00', '/build/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LANG="C"\n LC_ALL="C"\n SOURCE_DATE_EPOCH="1509789866"'), (1729687, 260743, 'i386', '0ad', 'Debian', 'i386', '2017-11-04T13:57:17+00:00', '/build/0ad-ARV4Xc/0ad-0.0.22', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1509789866"'), (1813337, 269932, 'kfreebsd-i386', '0ad', 'Debian', 'kfreebsd-i386', '2019-01-14T03:20:55+00:00', '/build/0ad-NyI3cw/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=2"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1547026389"'), (1822946, 271472, 'i386', '0ad', 'Debian', 'i386', '2019-01-04T12:20:19+00:00', '/build/0ad-u9Wzdv/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1546597099"'), (1823292, 271472, 'armhf', '0ad', 'Debian', 'armhf', '2019-01-04T16:36:52+00:00', '/build/0ad-4c732C/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1546597099"'), (1823724, 271472, 'amd64 source', '0ad', 'Debian', 'amd64', '2019-01-04T10:47:06+00:00', '/build/0ad-0.0.23.1', '\n CFLAGS=" -Wall -g -O2 -Wextra -pipe -funsigned-char -fstrict-aliasing -Wchar-subscripts -Wundef -Wshadow -Wcast-align -Wwrite-strings -Wunused -Wuninitialized -Wpointer-arith -Wredundant-decls -Winline -Wformat -Wformat-security -Wswitch-enum -Winit-self -Wmissing-include-dirs -Wempty-body -fdiagnostics-color=auto -Wmissing-prototypes -Wstrict-prototypes -Wold-style-definition -Wbad-function-cast -Wnested-externs  -Wmissing-declarations"\n CXXFLAGS=" -Wall -g -O2 -Wextra -pipe -funsigned-char -fstrict-aliasing -Wchar-subscripts -Wundef -Wshadow -Wcast-align -Wwrite-strings -Wunused -Wuninitialized -Wpointer-arith -Wredundant-decls -Winline -Wformat -Wformat-security -Wswitch-enum -Winit-self -Wmissing-include-dirs -Wempty-body -fdiagnostics-color=auto"\n DEB_BUILD_OPTIONS="parallel=5"\n LANG="C"\n LC_ALL="C"\n MAKEFLAGS="-j5"\n SOURCE_DATE_EPOCH="1546597099"'), (1827636, 269932, 'kfreebsd-amd64', '0ad', 'Debian', 'kfreebsd-amd64', '2019-01-09T13:08:17+00:00', '/build/0ad-3oVMdG/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=2"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1547026389"'), (1831072, 271472, 'kfreebsd-i386', '0ad', 'Debian', 'kfreebsd-i386', '2019-01-08T00:58:51+00:00', '/build/0ad-4CoCvU/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=2"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1546597099"'), (1831879, 271472, 'kfreebsd-amd64', '0ad', 'Debian', 'kfreebsd-amd64', '2019-01-06T09:15:41+00:00', '/build/0ad-r2tUKw/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=2"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1546597099"'), (1834601, 269932, 'armhf', '0ad', 'Debian', 'armhf', '2019-01-09T14:20:17+00:00', '/build/0ad-ipvuE6/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1547026389"'), (1834733, 269932, 'amd64 source', '0ad', 'Debian', 'amd64', '2019-01-09T09:48:29+00:00', '/build/0ad-0.0.23.1', '\n CFLAGS=" -Wall -g -O2 -Wextra -pipe -funsigned-char -fstrict-aliasing -Wchar-subscripts -Wundef -Wshadow -Wcast-align -Wwrite-strings -Wunused -Wuninitialized -Wpointer-arith -Wredundant-decls -Winline -Wformat -Wformat-security -Wswitch-enum -Winit-self -Wmissing-include-dirs -Wempty-body -fdiagnostics-color=auto -Wmissing-prototypes -Wstrict-prototypes -Wold-style-definition -Wbad-function-cast -Wnested-externs  -Wmissing-declarations"\n CXXFLAGS=" -Wall -g -O2 -Wextra -pipe -funsigned-char -fstrict-aliasing -Wchar-subscripts -Wundef -Wshadow -Wcast-align -Wwrite-strings -Wunused -Wuninitialized -Wpointer-arith -Wredundant-decls -Winline -Wformat -Wformat-security -Wswitch-enum -Winit-self -Wmissing-include-dirs -Wempty-body -fdiagnostics-color=auto"\n DEB_BUILD_OPTIONS="parallel=5"\n LANG="C"\n LC_ALL="C"\n MAKEFLAGS="-j5"\n SOURCE_DATE_EPOCH="1547026389"'), (1834839, 269932, 'i386', '0ad', 'Debian', 'i386', '2019-01-09T11:43:11+00:00', '/build/0ad-8c9Ndg/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1547026389"'), (1955998, 292253, 'i386', '0ad', 'Debian', 'i386', '2019-08-03T06:33:13+00:00', '/build/0ad-WlfHrO/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LANG="C.UTF-8"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1564802317"'), (1956255, 292253, 'amd64 source', '0ad', 'Debian', 'amd64', '2019-08-03T04:53:21+00:00', '/build/0ad-wbMtN7/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=8"\n LANG="de_DE.utf8"\n LC_ALL="C.UTF-8"\n LD_LIBRARY_PATH="/usr/lib/libeatmydata"\n MAKEFLAGS=" -j8"\n SOURCE_DATE_EPOCH="1564802317"'), (1956270, 292253, 'armhf', '0ad', 'Debian', 'armhf', '2019-08-03T06:52:57+00:00', '/build/0ad-IIMQpD/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=8"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1564802317"'), (1956526, 292415, 'amd64', '0ad', 'Debian', 'amd64', '2019-08-05T22:48:11+00:00', '/build/0ad-7RdEet/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1565024218"'), (1957005, 292415, 'i386', '0ad', 'Debian', 'i386', '2019-08-05T22:37:03+00:00', '/build/0ad-hk7Ds3/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LANG="C.UTF-8"\n LC_ALL="C.UTF-8"\n SOURCE_DATE_EPOCH="1565024218"'), (1958055, 292415, 'armhf', '0ad', 'Debian', 'armhf', '2019-08-05T23:59:09+00:00', '/build/0ad-0nrpbi/0ad-0.0.23.1', '\n DEB_BUILD_OPTIONS="parallel=4"\n LC_ALL="POSIX"\n SOURCE_DATE_EPOCH="1565024218"')]

checksum
[(90973, 94075368, '0ad-dbgsym_0.0.23.1-5+b1_armhf.deb', '73d9c0356a5206f1c16cc05844521558', '3e1095ba05c94b7c2a6e0c7959daa42c124c6409', '87578f6d853ea64d8f30d98329e349e0ecd3b9a56dee8cadf586314eca4b141f'), (90973, 4868924, '0ad_0.0.23.1-5+b1_armhf.deb', '802e8c061186e4061d4498558273d2a4', '7ea79a931ec33b787e1b67f4174e22507da97867', 'e53828b4515f268ec960f090e6202b58f3ddbccb5a65520e5bbad5e1fefaca91'), (106980, 107547664, '0ad-dbgsym_0.0.23.1-5+b1_i386.deb', '1e7bc1043143f1679c8f377bf1772cff', '08481635c8d2b5b4e1bad8d7adbdd98b9a83a8d4', '1274cd8bd1b2a91ae797f2d53514a49525b61bb36f4688fefd2855a153ae0c16'), (106980, 5875076, '0ad_0.0.23.1-5+b1_i386.deb', '6aa7ddbead91129a020d66b14bb2057c', '131815bd8c0e56e00617c778adc30df17f750cd9', '5deb702574a7421f08d57094d1b53f25eb5d997d133edf27d4b5a338a420c05c'), (107946, 112132472, '0ad-dbgsym_0.0.23.1-5+b1_amd64.deb', 'e6bd28d530c621b9d351d77c102594a8', 'c828c3f5f252ca7b0aecfcfcffdbcb50b8cd0eab', '8cc1a23d2b7460f17123d7a0ad9709b7e526acc70bfb04b530fe27814e4f4b9e'), (107946, 5588508, '0ad_0.0.23.1-5+b1_amd64.deb', '35412374733ae00cbbc7260596e1d78c', '38691afb38ff0396afc8a0884fb4343b979c1eae', '610e9f9c41be18af516dd64a6dc1316dbfe1bb8989c52bafa556de9e381d3e29')]
*/

struct table_row {
    source_id: u64,
    source_name: String,
    source_version: String,
    buildinfo_id: u64,
    buildinfo_arch: String,
    build_date: DateTime<Utc>,
    binary_id: u64,
    binary_package: String,
    binary_version: String,
    build_sha256: String,
    publish_package: String,
    publish_version: String,
    publish_sha256: String
}

struct pub_row {
    package_id: u64,
    package: String,
    architecture: String,
    version: String,
    release: String,
    section: String,
    size: String,
    pool_endpoint: String,
    dfsg: String,
    time: String,
    md5: String,
    sha: String,
    provided_by: String,
}

#[derive(Debug)]
struct source_row {
    source_id: u64,
    source_name: String,
    version: String,
}

#[derive(Debug)]
struct buildinfo_row {
    buildinfo_id: u64,
    source_id: u64,
    architecture: String,
    source_raw: String,
    build_origin: String,
    build_architecture: String,
    build_date: DateTime<Utc>,
    build_path: String,
    environment: String,
}

#[derive(Debug)]
struct checksum_row {
    buildinfo_id: u64,
    file_size: u64,
    file_name: String,
    checksum_md5: String,
    checksum_sha1: String,
    checksum_sha256: String,
}

impl table_row {
    fn to_string(&self) -> String {
        format!(
            "source_id: {}, source_name: {}, source_version: {}, buildinfo_id: {}, buildinfo_arch: {}, build_date: {}, binary_id: {}, binary_package: {}, binary_version: {}, build_sha: {}, publish_package: {}, publish_version: {}, publish_sha: {}",
            self.source_id,
            self.source_name,
            self.source_version,
            self.buildinfo_id,
            self.buildinfo_arch,
            self.build_date,
            self.binary_id,
            self.binary_package,
            self.binary_version,
            self.build_sha256,
            self.publish_package,
            self.publish_version,
            self.publish_sha256
        )
    }
    fn compare_shas(&self) -> bool {
        if self.build_sha256.is_empty() || self.publish_sha256.is_empty() {
            return false;
        }
        return self.build_sha256 == self.publish_sha256;
    }
}

fn query_print(conn: &Connection, qr: &str) -> Result<()> {
    let mut stmt = conn.prepare(qr)?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        println!("{:?}", row);
    }

    Ok(())
}

fn query_ret(conn: &Connection, qr: &str) -> Result<Vec<table_row>> {
    let mut stmt = conn.prepare(qr)?;
    let mut rows = stmt.query([])?;

    let mut results = Vec::new();

    while let Some(row) = rows.next()? {
        let tmp_date: String = row.get(5)?;
        let parsed: DateTime<Utc> = tmp_date.parse().unwrap();
        let r = table_row {
            source_id: row.get::<_, Option<u64>>(0)?.unwrap_or(0),
            source_name: row.get::<_, Option<String>>(1)?.unwrap_or("".to_string()),
            source_version: row.get::<_, Option<String>>(2)?.unwrap_or("".to_string()),
            buildinfo_id: row.get::<_, Option<u64>>(3)?.unwrap_or(0),
            buildinfo_arch: row.get::<_, Option<String>>(4)?.unwrap_or("".to_string()),
            build_date: parsed,
            binary_id: row.get::<_, Option<u64>>(6)?.unwrap_or(0),
            binary_package: row.get::<_, Option<String>>(7)?.unwrap_or("".to_string()),
            binary_version: row.get::<_, Option<String>>(8)?.unwrap_or("".to_string()),
            build_sha256: row.get::<_, Option<String>>(9)?.unwrap_or("".to_string()),
            publish_package: row.get::<_, Option<String>>(10)?.unwrap_or("".to_string()),
            publish_version: row.get::<_, Option<String>>(11)?.unwrap_or("".to_string()),
            publish_sha256: row.get::<_, Option<String>>(12)?.unwrap_or("".to_string()),
        };
        results.push(r);
    }

    Ok(results)
}

fn query_bin(conn: &Connection, qr: &str) -> Result<Vec<pub_row>> {
    let mut stmt = conn.prepare(qr)?;
    let mut rows = stmt.query([])?;

    let mut results = Vec::new();

    while let Some(row) = rows.next()? {
        let r = pub_row {
            package_id: row.get::<_, Option<u64>>(0)?.unwrap_or(0),
            package: row.get::<_, Option<String>>(1)?.unwrap_or("".to_string()),
            architecture: row.get::<_, Option<String>>(2)?.unwrap_or("".to_string()),
            version: row.get::<_, Option<String>>(3)?.unwrap_or("".to_string()),
            release: row.get::<_, Option<String>>(4)?.unwrap_or("".to_string()),
            section: row.get::<_, Option<String>>(5)?.unwrap_or("".to_string()),
            size: row.get::<_, Option<String>>(6)?.unwrap_or("".to_string()),
            pool_endpoint: row.get::<_, Option<String>>(7)?.unwrap_or("".to_string()),
            dfsg: row.get::<_, Option<String>>(8)?.unwrap_or("".to_string()),
            time: row.get::<_, Option<String>>(9)?.unwrap_or("".to_string()),
            md5: row.get::<_, Option<String>>(10)?.unwrap_or("".to_string()),
            sha: row.get::<_, Option<String>>(11)?.unwrap_or("".to_string()),
            provided_by: row.get::<_, Option<String>>(12)?.unwrap_or("".to_string()),
        };
        results.push(r);
    }

    Ok(results)
}

fn query_source(conn: &Connection, qr: &str) -> Result<Vec<source_row>> {
    let mut stmt = conn.prepare(qr)?;
    let mut rows = stmt.query([])?;

    let mut results = Vec::new();

    while let Some(row) = rows.next()? {
        let r = source_row {
            source_id: row.get::<_, Option<u64>>(0)?.unwrap_or(0),
            source_name: row.get::<_, Option<String>>(1)?.unwrap_or("".to_string()),
            version: row.get::<_, Option<String>>(2)?.unwrap_or("".to_string()),
        };
        results.push(r);
    }

    Ok(results)
}

fn query_buildinfo(conn: &Connection, qr: &str) -> Result<Vec<buildinfo_row>> {
    let mut stmt = conn.prepare(qr)?;
    let mut rows = stmt.query([])?;

    let mut results = Vec::new();

    while let Some(row) = rows.next()? {
        let tmp_date: String = row.get(6)?;
        let parsed: DateTime<Utc> = tmp_date.parse().unwrap();
        let r = buildinfo_row {
            buildinfo_id: row.get::<_, Option<u64>>(0)?.unwrap_or(0),
            source_id: row.get::<_, Option<u64>>(1)?.unwrap_or(0),
            architecture: row.get::<_, Option<String>>(2)?.unwrap_or("".to_string()),
            source_raw: row.get::<_, Option<String>>(3)?.unwrap_or("".to_string()),
            build_origin: row.get::<_, Option<String>>(4)?.unwrap_or("".to_string()),
            build_architecture: row.get::<_, Option<String>>(5)?.unwrap_or("".to_string()),
            build_date: parsed,
            build_path: row.get::<_, Option<String>>(7)?.unwrap_or("".to_string()),
            environment: row.get::<_, Option<String>>(8)?.unwrap_or("".to_string()),
        };
        results.push(r);
    }

    Ok(results)
}

fn query_checksum(conn: &Connection, qr: &str) -> Result<Vec<checksum_row>> {
    let mut stmt = conn.prepare(qr)?;
    let mut rows = stmt.query([])?;

    let mut results = Vec::new();

    while let Some(row) = rows.next()? {
        let r = checksum_row {
            buildinfo_id: row.get::<_, Option<u64>>(0)?.unwrap_or(0),
            file_size: row.get::<_, Option<u64>>(1)?.unwrap_or(0),
            file_name: row.get::<_, Option<String>>(2)?.unwrap_or("".to_string()),
            checksum_md5: row.get::<_, Option<String>>(3)?.unwrap_or("".to_string()),
            checksum_sha1: row.get::<_, Option<String>>(4)?.unwrap_or("".to_string()),
            checksum_sha256: row.get::<_, Option<String>>(5)?.unwrap_or("".to_string()),
        };
        results.push(r);
    }

    Ok(results)
}


fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    let conn = Connection::open("/home/cyberg/debtrace/data/debtrace.db")?;
    let package = &args[1];

    let pub_query = format!("SELECT * FROM Publish_Packages WHERE package LIKE '%{}%';", package);
    let published: Result<Vec<pub_row>, rusqlite::Error> = query_bin(&conn, &pub_query);

    for publication in published? {
        println!("checking package {} version {}", publication.package, publication.version);
        //version formatting different         following taken out from below:  AND version = '{}'    publication.version
        let source_query = format!("SELECT * FROM source_table WHERE source_name = '{}';", publication.package);
        let sources = query_source(&conn, &source_query)?;
        if sources.is_empty() {
            println!("no source");
            continue;
        }
        let source = &sources[0];
        println!("source found {:?}", source);
        //this is fine
        let build_info_query = format!("SELECT * FROM buildinfo_table WHERE source_id = '{}';", source.source_id);
        let buildinfos = query_buildinfo(&conn ,&build_info_query)?;
        if buildinfos.is_empty() {
            println!("no buildinfo");
            continue;
        }
        println!("buildinfo found {:?}", buildinfos);
        let buildinfo = &buildinfos[0];
        //could find none for 0ad? the names are a different format but matching by buildinfo_id is not good
        let checksum_query = format!("SELECT * FROM checksum_table WHERE buildinfo_id = '{}';", buildinfo.buildinfo_id);
        let checksums = query_checksum(&conn, &checksum_query)?;
        if checksums.is_empty() {
            println!("no checksums found");
            continue;
        } 
        println!("checksums: {:?}", checksums);
        let checksum = &checksums[0];
        if checksum.checksum_md5 == publication.md5 && checksum.checksum_sha256 == publication.sha {
            println!("\npath found for {} version {} with hashes:\nmd5: {}\nsha: {}\n\n", publication.package, publication.version, publication.md5, publication.sha);
        } else {
            println!("hashes missmatched for {} version {}\nmd5: {} -- {}\nsha: {} -- {}", publication.package, publication.version, publication.md5, checksum.checksum_md5, publication.sha, checksum.checksum_sha256);
        }

    }

    Ok(())
}
