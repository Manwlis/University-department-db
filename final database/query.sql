
-- 1.1 ------------------------------------------------------------------------------------------------------------
--	Epi8eta
CREATE OR REPLACE FUNCTION random_surnames(n integer) RETURNS TABLE(surname character(50), id integer) AS $$

BEGIN
	RETURN QUERY
	SELECT snam.surname, row_number() OVER ()::integer
	FROM (
		SELECT "Surname".surname 
		FROM "Surname"
		WHERE right("Surname".surname,2)='ΗΣ' OR right("Surname".surname,2)='ΟΣ'
		ORDER BY random() LIMIT n
	) as snam;
END;

$$ LANGUAGE 'plpgsql' VOLATILE;

select random_surnames(54) 

-- onomata
CREATE OR REPLACE FUNCTION random_names(n integer) RETURNS TABLE(name character(30),sex character(1), id integer) AS $$

BEGIN
RETURN QUERY
	SELECT nam.name, nam.sex, row_number() OVER ()::integer
	FROM (SELECT "Name".name, "Name".sex FROM "Name" ORDER BY random() LIMIT n) as nam;
END;

$$ LANGUAGE 'plpgsql' VOLATILE; 

select random_names(54) 

-- onomata patros
CREATE OR REPLACE FUNCTION random_father_names(n integer) RETURNS TABLE(name character(30),sex character(1), id integer) AS $$

BEGIN
RETURN QUERY
	SELECT nam.name, nam.sex, row_number() OVER ()::INTEGER 					--mono arsenika onomata
	FROM (SELECT "Name".name, "Name".sex FROM "Name" WHERE "Name".sex='M' ORDER BY random() LIMIT n) as nam;
END;

$$ LANGUAGE 'plpgsql' VOLATILE; 

select random_father_names(54) 

--  8ulhka epi8eta
CREATE OR REPLACE FUNCTION adapt_surname(surname character(50), sex character(1)) RETURNS character(50) AS $$

DECLARE
	result character(50);
BEGIN
	result = surname;
	IF right(surname,2)<>'ΗΣ' and right(surname,2)<>'ΟΣ' THEN
		RAISE NOTICE 'Cannot handle this surname';
	ELSIF sex='F' and right(surname,2)='ΗΣ' THEN
		result = left(surname,-1);
	ELSIF sex='F'and right(surname,2)='ΟΣ' THEN
		result = left(surname,-1);
		result = result || 'Υ';
	ELSIF sex<>'M' THEN
		RAISE NOTICE 'Wrong sex parameter';
	END IF;
RETURN result;
END;

$$ LANGUAGE 'plpgsql' IMMUTABLE;

SELECT adapt_surname('ΠΑΠΑΓΟΣ', 'F')


-- 1.1.ka8hghtes
CREATE OR REPLACE FUNCTION create_professor(num integer) RETURNS void AS $$

	INSERT INTO "Professor"
	SELECT amka, name, father_name, surname,
		('p' || sem.academic_year::character(4) || lpad(amka::text,6,'0') || '@isc.tuc.gr') as email, 	-- ftiaxnoume to email  ws p+twrinh xronia+amka padded+kataliksh
		floor(random() * 10 + 1)::int as labJoins, 														-- tuxaio labjoin
		(enum_range(null::rank_type))[ceil(random()*4)] as rank											--pernoume tuxaio rank
	FROM "Semester" sem,(
		SELECT n.id + max(p.amka) as amka, n.name, fn.name as father_name, adapt_surname(s.surname, n.sex) as surname --ftiaxnoume amka, onomateponimo kai mpampa
		FROM random_names(num) n NATURAL JOIN random_surnames(num) s , random_father_names(num) fn, "Professor" p
		WHERE fn.id = n.id
		GROUP BY n.name, fn.name, adapt_surname(s.surname, n.sex), n.id
		order by amka
	)AS temp_name
	WHERE sem.semester_status = 'present'
	order by amka

$$ LANGUAGE SQL;

SELECT create_professor(20)


-- 1.1.labstaff
CREATE OR REPLACE FUNCTION create_LabStaff(num integer) RETURNS void AS $$

	INSERT INTO "LabStaff"
	SELECT amka, name, father_name, surname,
		('l' || sem.academic_year::character(4) || lpad(amka::text,6,'0') || '@isc.tuc.gr') as email, 					-- ftiaxnoume email: 'l'+twrinh xronia+amka padded
		floor(random() * 10 + 1)::int as labJoins, 																		-- tuxaio labjoin
		(enum_range(null::level_type))[ceil(random()*4)] as rank														-- pernoume tuxaio rank
	FROM "Semester" sem,(
		SELECT n.id + max(lb.amka) as amka, n.name, fn.name as father_name, adapt_surname(s.surname, n.sex) as surname 	-- ftiaxnoume amka, onomateponimo kai mpampa
		FROM random_names(num) n NATURAL JOIN random_surnames(num) s , random_father_names(num) fn, "LabStaff" lb
		WHERE fn.id = n.id
		GROUP BY n.name, fn.name, adapt_surname(s.surname, n.sex), n.id
		order by amka
	)AS temp_name
	WHERE sem.semester_status = 'present'
	order by amka

$$ LANGUAGE SQL;

SELECT create_LabStaff(20)


-- 1.1.foitites 
-- find ari8mos mitrwou
CREATE OR REPLACE FUNCTION find_years_max_am(year integer) RETURNS integer AS $$

DECLARE
	result integer;
BEGIN
	result =(SELECT max(am::integer) FROM "Student" s WHERE left(s.am,4) = year::text); -- epistrefei ton am tou teleutaiou ths xronias
	IF result IS NULL THEN 																-- an den iparxei kaneis se authn th xronia
	result = year * 1000000;
	END IF;
RETURN result;
END;

$$ LANGUAGE 'plpgsql' IMMUTABLE; 

SELECT find_years_max_am(2019)


CREATE OR REPLACE FUNCTION create_Students(num integer, entry_date date) RETURNS void AS $$

	INSERT INTO "Student"
	SELECT amka, name, father_name, surname,
		('s' || EXTRACT(YEAR FROM entry_date)::character(4) || lpad(amka::text,6,'0') || '@isc.tuc.gr') as email, 		-- ftiaxnoume email: 's'+xronia eisagwghs+amka padded
		(find_years_max_am(EXTRACT(YEAR FROM entry_date)::integer) + id)::character(10) as am, 							-- am ftiaxnete pros8etontas to id sto am tou max ths xronias
		entry_date																										-- pernoume entry date
	FROM(
		SELECT n.id + max(st.amka) as amka, n.name, fn.name as father_name, adapt_surname(s.surname, n.sex) as surname, n.id 	-- ftiaxnoume amka, onomateponimo kai mpampa
		FROM random_names(num) n NATURAL JOIN random_surnames(num) s , random_father_names(num) fn, "Student" st
		WHERE fn.id = n.id
		GROUP BY n.name, fn.name, adapt_surname(s.surname, n.sex), n.id
		order by amka
	)AS temp_name
	order by amka

$$ LANGUAGE SQL;

SELECT create_Students(5,'2051-09-10')
delete from "Student" where amka>= 137



--1.2 ------------------------------------------------------------------------------------------------------------
-- briskei bash ergasthriou
CREATE OR REPLACE FUNCTION get_lab_min(s_number integer, c_code character) RETURNS numeric AS $$

DECLARE
	result numeric;
BEGIN
	result = (select lab_min from "CourseRun" where c_code = course_code and serial_number = s_number);
return result;
end;

$$ LANGUAGE 'plpgsql';	

-- briskei bash exetastikhs
CREATE OR REPLACE FUNCTION get_exam_min(s_number integer, c_code character) RETURNS numeric AS $$

DECLARE
	result numeric;
BEGIN
	result = (select exam_min from "CourseRun" where c_code = course_code and serial_number = s_number);
return result;
end;

$$ LANGUAGE 'plpgsql';	


-- ipologizei ton teliko ba8mo
CREATE OR REPLACE FUNCTION calculate_final_grade(cr_code character, s_number integer, exam_grade numeric, lab_grade numeric)
RETURNS numeric AS $$

DECLARE
	result numeric;
BEGIN
	if lab_grade is null then
		result = exam_grade;
	else
		result = exam_grade * (select c.exam_percentage from "CourseRun" c where cr_code = c.course_code and s_number = c.serial_number)
		+ lab_grade * (select (1-c.exam_percentage) from "CourseRun" c where cr_code = c.course_code and s_number = c.serial_number);
	end if;
RETURN result;
END;

$$ LANGUAGE 'plpgsql' IMMUTABLE;


-- bazei ba8mous, xrisimopoiei tis apo panw
CREATE OR REPLACE FUNCTION update_grades() 
RETURNS void AS $$

	-- ba8mos ergasthriou
	UPDATE "Register" r SET lab_grade=floor(random() * 10 + 1)
	WHERE r.register_status = 'approved' AND r.lab_grade is NULL  AND r.course_code IN (
		SELECT c.course_code	-- krataei ta ma8hmata pou exoun ergasthrio
		FROM "Course" c
		WHERE c.lab_hours != 0
	);
	UPDATE "Register" r SET exam_grade = 0, final_grade = 0, register_status = 'fail'		-- midenizoume grapto kai teliko, kai kanontas to fail den mporei na to ksanaalaksei
	WHERE r.register_status = 'approved' AND r.lab_grade < get_lab_min(r.serial_number, r.course_code) -- pernoume tis baseis twn ergasthriwn
	AND r.course_code IN (
		SELECT c.course_code	-- krataei ta ma8hmata pou exoun ergasthrio
		FROM "Course" c
		WHERE c.lab_hours != 0
	); 

	-- ba8mos e3etashs
	UPDATE "Register" r SET exam_grade = floor(random() * 10 + 1)
	WHERE r.register_status = 'approved' AND r.exam_grade is NULL;	-- ola ta ma8hmata tou eksaminou exoun ba8mo eksetashs ara den prepei na tsekaroume pia 8a piraksoume
	UPDATE "Register" r set final_grade = exam_grade, register_status = 'fail'		-- midenizoume grapto kai teliko, kai kanontas to fail den mporei na to ksanaalaksei
	WHERE r.register_status = 'approved' AND r.exam_grade < get_exam_min(r.serial_number, r.course_code); -- pernoume tis baseis twn ergasthriwn						
								
	-- telikos ba8mos
	UPDATE "Register" r SET final_grade = calculate_final_grade(r.course_code, r.serial_number, r.exam_grade, r.lab_grade)
	WHERE r.register_status = 'approved';
	UPDATE "Register" r SET register_status = 'fail'
	WHERE r.register_status = 'approved' and r.final_grade < 5;
	UPDATE "Register" r SET register_status = 'pass'
	WHERE r.register_status = 'approved' and r.final_grade >= 5;

$$ LANGUAGE SQL;	

select update_grades()
select* from "Register" where serial_number = 22 and register_status != 'rejected'


-- 1.3 ------------------------------------------------------------------------------------------------------------
-- xriazetai na ftiaksoume thn update_grades()
CREATE OR REPLACE FUNCTION update_thesis_grade() RETURNS void AS $$

	UPDATE "Diploma" d SET thesis_grade = floor(random() * 6 + 5)
	WHERE d.amka IN (
		SELECT s.amka
	    FROM "Student" s NATURAL join "Diploma" d 
	    WHERE d.thesis_grade IS NULL and s.entry_date <= (
			Select end_date 							-- an exei egrafei prin apo authn thn hmeromhnia, exei oloklhrwsei 10 eksamina
			from "Semester" 
			where semester_id = (select semester_id from "Semester" where semester_status = 'present') - 10 
		)
	)
		
$$ LANGUAGE SQL;	

SELECT update_thesis_grade()

--gia na doume oti douleuei to 1.3 , prwta kanoume auto 
UPDATE "Diploma" d 
set thesis_grade = null
WHERE d.amka > 30


-- 1.4 ------------------------------------------------------------------------------------------------------------
-- trigger gia na einai o pinakas diploma sunephs
CREATE OR REPLACE FUNCTION update_diploma_on_sem_change() RETURNS TRIGGER AS $$
BEGIN

insert into "Diploma"
select foitites.amka, null, ('Διπλωματική εργασία' || rpad( foitites.name, 30, ' ' ) || foitites.surname) as thesis_title, null, null, null, 
floor(random() * (max(p.amka) - min(p.amka + 1)) + min(p.amka))::int, floor(random() * (max(p.amka) - min(p.amka + 1)) + min(p.amka))::int,
floor(random() * (max(p.amka) - min(p.amka + 1)) + min(p.amka))::int
from
(
	select amka, name, surname
	from  "Student"
	where entry_date < (select end_date from "Semester" where semester_id = old.semester_id - 9) and amka not in (select amka from "Diploma")
)as foitites,"Professor" p
group by foitites.amka, thesis_title
order by foitites.amka;
	
RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';
-- trigger
CREATE TRIGGER diploma_update
BEFORE Update ON "Semester"
FOR EACH ROW 
when (old.semester_status = 'future' and new.semester_status = 'present')
EXECUTE PROCEDURE update_diploma_on_sem_change()

update "Semester" set semester_status = 'present' where semester_id = 24


-- ipologizei suntelesth barititas
CREATE OR REPLACE FUNCTION baritita_ma8hmatos(units integer) RETURNS numeric AS $$
DECLARE
	result numeric;
BEGIN
	if units < 3 then
		result =  1;
	elsif units > 4 then
		result = 2;
	else
		result = 1.5;
	end if;
RETURN result;
END;
$$ LANGUAGE 'plpgsql';	


CREATE OR REPLACE FUNCTION anakiriksh_apofoitwn(grad_date date) RETURNS void AS $$
-- an iparxoun ston pinaka
UPDATE "Diploma" SET  (diploma_grade,  diploma_num, graduation_date) =
(
	SELECT diploma_grade, diploma_num, grad_date
	FROM 
	(
		select d.amka, mesos_oros * 0.8 + thesis_grade * 0.2 as diploma_grade, ((select max(diploma_num) from "Diploma") + row_number() OVER ()::integer) as diploma_num	--ipologismos ba8mou ptixiou kai ari8mou ptixiou
		from "Diploma" d natural join
		(
			select amka, sum(ba8mos_barititas) / sum(baritita_ma8hmatos) as mesos_oros	--ipologismos mesou orou ma8hmatwn
			from
			(					
				select x.amka, course_code, units, baritita_ma8hmatos, ba8mos_barititas, final_grade, obligatory, running_total, row_number				-- kratame ta ma8hata pou prepei
				from "Graduation_rules", "Student" s,
				(																
					select s.amka, c.course_code, units, baritita_ma8hmatos(units), baritita_ma8hmatos(units) * final_grade as ba8mos_barititas, 
						final_grade, obligatory,
						sum(units) over (PARTITION BY amka order by obligatory desc, final_grade desc, units desc, c.course_code) as running_total, 
						row_number() OVER (PARTITION BY amka ORDER BY amka asc, obligatory desc, final_grade desc, units desc)::integer
					from "Register" r natural join "Student" s , "CourseRun" c natural join "Course"
					where r.serial_number = c.serial_number and r.course_code = c.course_code and r.register_status = 'pass' and s.am in (select get_den_exoun_apofitisei())
					order by amka asc, obligatory desc, final_grade desc, units desc
				)as x						
				where (units >= min_units or row_number <= (min_courses + 35)) and s.amka = x.amka and EXTRACT(YEAR from s.entry_date) = year_rules -- na einai ta 35 ipoxrwtika + ta epiloghs ths xronias tou  kai na simplirwnoun tis dm
				order by amka asc, obligatory desc, final_grade desc, units desc					
			)as x
			group by amka
		)as mesoi_oroi
	)as telikoi_ba8moi
where "Diploma".amka = telikoi_ba8moi.amka
)
where graduation_date IS NULL
$$
LANGUAGE SQL;	

SELECT anakiriksh_apofoitwn('2020-05-01')

UPDATE "Diploma" SET  graduation_date = NULL
where amka =1



-- 1.5 ------------------------------------------------------------------------------------------------------------.
CREATE OR REPLACE FUNCTION dhmiourgia_proteinomenwn() RETURNS void AS $$

insert into "Register"
select  amka, semester_id, course_code, NULL, NULL, NULL, 'proposed'
from
(
	select amka, semester_id, course_code
	from
	(
		select course_code, typical_year, semester_id	-- ma8imata pou ginontai auto to eksamino kai pia einai h xronia tous
		from "Semester", "CourseRun" natural join "Course"
		where semester_status = 'present' and serial_number = semester_id
	) as ma8hmata,
	(
		select amka, ((select semester_id from "Semester" where semester_status = 'present') - semester_id) / 2 as etos_spoudwn		--etos spoudwn ka8e foitith
		from "Student", "Semester"
		where EXTRACT(YEAR from entry_date) = academic_year and academic_season = 'winter' and amka not in( select amka from "Diploma" where graduation_date is not NULL)
		-- den ftiaxnoume registers gia foitites pou exoun apofoitisei
	) as foitites
	where typical_year <= etos_spoudwn 
)as x
where  NOT EXISTS 
(
	SELECT * FROM "Register" r
    WHERE (r.amka = x.amka AND r.course_code = x.course_code and r.register_status = 'pass')			-- na mhn ta exei perasei
		or (r.amka = x.amka AND r.course_code = x.course_code and r.serial_number = x.semester_id ) 	-- na mhn iparxei register me idia amka, ma8hma, xronia
) 

$$
LANGUAGE SQL;	

select dhmiourgia_proteinomenwn()
select * from "Register" where serial_number = 23
update "Semester" set  semester_status = 'present' where semester_id = 22


-- 1.6 ------------------------------------------------------------------------------------------------------------
-- elenxos an to semester einai future
CREATE OR REPLACE FUNCTION check_CourseRun_ins() RETURNS TRIGGER AS $$
BEGIN
	if new.serial_number <= (Select semester_id from "Semester" where semester_status = 'present') then
		RAISE EXCEPTION  'Παραβιάζεται η χρονική σειρά.';
	end if;
RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';	

-- trigger gia insert sto CourseRun
CREATE TRIGGER check_CourseRun_ins
BEFORE INSERT ON "CourseRun"
FOR EACH ROW 
EXECUTE PROCEDURE check_CourseRun_ins()

--
CREATE OR REPLACE FUNCTION dhmiourgia_CourseRun(eksamino integer) RETURNS void AS $$
-- bazei nea course run. An paei na balei se eksamino pou den einai future skaei to trigger
insert into "CourseRun"
select cr.course_code, semester_id, exam_min, lab_min, exam_percentage, labuses, semester_id, amka_prof1, amka_prof2
from "CourseRun" cr natural join "Course", "Semester"
where serial_number >= ALL(select serial_number from "CourseRun" x where cr.course_code = x.course_code) 	-- plirofories apo prohgoumena courserun
	and typical_season = academic_season and semester_id = eksamino											-- eksamino kai xronia
	and NOT EXISTS 
(
	SELECT * FROM "CourseRun" x
    WHERE x.course_code = cr.course_code and x.serial_number = semester_id 									-- na mhn uparxei idio course thn idia xronia
) 
$$ LANGUAGE SQL;	

--
select * from dhmiourgia_CourseRun(25)
--
update "Semester" set  semester_status = 'present' where semester_id = 22
--
delete from "CourseRun"
where serial_number = 25
-- 
select * 
from "CourseRun"
where serial_number = 25


-- 2.1 ------------------------------------------------------------------------------------------------------------
-- 2.1.a
CREATE OR REPLACE FUNCTION get_Professor (l_code INTEGER) 
RETURNS TABLE(name character, surname character, email character) AS $$

SELECT name, surname, email
FROM "Professor" p, "Lab" l
WHERE p.amka = l.profdirects and l.lab_code = l_code
	
$$ LANGUAGE SQL;

SELECT get_professor(5)


-- 2.1.b
CREATE OR REPLACE FUNCTION get_Professor (crs_code character, ac_year integer, ac_season semester_season_type) 
RETURNS TABLE(name character, surname character, email character) AS $$

SELECT name, surname, email
FROM "Professor" p
WHERE p.amka = 
(
	SELECT amka_prof1
	FROM "CourseRun" cr natural join "Course" c
	WHERE cr.course_code = crs_code AND cr.semesterrunsin = ac_year and c.typical_season = ac_season
)

UNION

SELECT name, surname, email
FROM "Professor" p
WHERE p.amka = 
(
	SELECT amka_prof2
	FROM "CourseRun" cr natural join "Course" c
	WHERE cr.course_code = crs_code AND cr.semesterrunsin = ac_year and c.typical_season = ac_season
)
$$ LANGUAGE SQL;

SELECT get_professor('ΠΛΗ 101', 15, 'winter')


-- 2.2 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_Grades (crs_code character, semesterrunsin integer, typical_season semester_season_type, ba8mologia text) 
RETURNS TABLE(name character, surname character, am character, grade numeric) AS $$

BEGIN
	CASE ba8mologia
		WHEN 'exam' THEN
			RETURN QUERY
			SELECT DISTINCT  s.name, s.surname, s.am , r.exam_grade
			FROM "Student" s , "Register" r, "Semester" sem
			WHERE s.amka = r.amka and r.serial_number = sem.semester_id AND sem.academic_year = semesterrunsin 
			AND sem.academic_season = typical_season and r.course_code = crs_code;
		WHEN 'lab' THEN
			RETURN QUERY
			SELECT DISTINCT  s.name, s.surname, s.am , r.lab_grade
			FROM "Student" s , "Register" r, "Semester" sem
			WHERE s.amka = r.amka and r.serial_number = sem.semester_id AND sem.academic_year = semesterrunsin 
			AND sem.academic_season = typical_season and r.course_code = crs_code;
		WHEN 'final' THEN
			RETURN QUERY
			SELECT DISTINCT  s.name, s.surname, s.am , r.final_grade
			FROM "Student" s , "Register" r, "Semester" sem
			WHERE s.amka = r.amka and r.serial_number = sem.semester_id AND sem.academic_year = semesterrunsin 
			AND sem.academic_season = typical_season and r.course_code = crs_code;
	END CASE;
END;

$$ LANGUAGE plpgsql;

select get_Grades('ΗΡΥ 204', 2012, 'spring', 'final')


-- 2.3 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_Professor_of_rank (prof_rank rank_type) 
RETURNS TABLE(name character, surname character, sector_title character) AS $$

SELECT p.name, p.surname, s.sector_title
FROM "Professor" p, "Sector" s NATURAL JOIN "Lab" l 
WHERE p.rank = prof_rank AND (p.amka = l.profdirects OR p."labJoins" = l.lab_code)	-- ta rank pou zitaei, oi ka8hghtes pou summetexoun se ergasthrio
AND p.amka IN(																		-- didaskoun to trexon eksamino
	SELECT cr.amka_prof1							
	FROM "CourseRun" cr, "Semester" s
	WHERE cr.semesterrunsin = s.semester_id AND semester_status = 'present'
																
	UNION
																					
	SELECT cr.amka_prof2
	FROM "CourseRun" cr, "Semester" s
	WHERE cr.semesterrunsin = s.semester_id AND semester_status = 'present'
)

$$ LANGUAGE SQL;

select get_Professor_of_rank('full')


-- 2.4 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_Course_happ () 
RETURNS TABLE(course_code character, course_title character,happens character(3)) AS $$

	SELECT DISTINCT  b.course_code, b.course_title, 'NAI'			-- ola osa ginonai
	FROM "Course" b natural join "CourseRun" cr, "Semester" s
	WHERE cr.semesterrunsin = s.semester_id AND s.semester_status = 'present'
		
UNION

(
	SELECT distinct d.course_code, d.course_title,'OXI'				-- ola tou eksamhnou
	FROM "Course" d natural join "CourseRun" cr, "Semester" s
	WHERE cr.semesterrunsin = s.semester_id  AND s.academic_season = (select academic_season from "Semester" where semester_status = 'present')

EXCEPT
		
	SELECT distinct d.course_code, d.course_title,'OXI'				-- ola osa ginonai
	FROM "Course" d natural join "CourseRun" cr, "Semester" s
	WHERE s.academic_season = 'spring' AND s.semester_status = 'present' and s.semester_id = cr.semesterrunsin
)

$$ LANGUAGE SQL;

SELECT get_Course_happ ()


-- 2.5 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_oblig_courses (stud_amka integer) 
RETURNS TABLE(course_code character, course_title character) AS $$

	SELECT c.course_code, c.course_title 
	FROM "Course" c
	WHERE obligatory = 'true' AND c.course_code NOT IN(			-- ipoxrewtika opou den uparxei sta
		SELECT r.course_code
		FROM "Register" r
		WHERE register_status = 'pass' and amka = stud_amka)	-- registers me perasmena ma8hmata kai to amka tou ma8hth
	
$$ LANGUAGE SQL;

select get_oblig_courses(4)
		
		
-- 2.6 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_Sector_Max() RETURNS character AS $$

	SELECT val.sector_title
	FROM(
		SELECT sector_title, count(*) as cnt
		FROM(
			SELECT s.sector_title
			FROM "Diploma" d, "Professor" p, "Lab" l, "Sector" s
			WHERE d.amka_super = p.amka AND p."labJoins" = l.lab_code AND l.sector_code = s.sector_code
		)AS dal  GROUP BY sector_title
	)AS val GROUP BY sector_title,val.cnt
	HAVING val.cnt = (
		SELECT MAX(val.cnt)
		FROM(
			SELECT count(*) as cnt
			FROM(
				SELECT s.sector_title
				FROM "Diploma" d, "Professor" p, "Lab" l, "Sector" s
				WHERE d.amka_super = p.amka AND p."labJoins" = l.lab_code AND l.sector_code = s.sector_code
	    	)AS dal GROUP BY sector_title
		)AS val
	)

$$ LANGUAGE SQL;

SELECT get_Sector_Max()

-- 2.7 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_percentage (acad_year integer, acad_season semester_season_type) 
RETURNS TABLE(course_code character, course_title character, percentage numeric) AS $$

	SELECT r.course_code, crs.course_title, 
		100 * (
		count(CASE WHEN r.register_status = 'pass' THEN 1 ELSE NULL END)::numeric							-- epitixies
		/ 
		count(CASE WHEN r.register_status = 'pass' or r.register_status = 'fail' THEN 1 ELSE NULL END)		-- epitixies kai apotixies
		)
	FROM "Register" r NATURAL JOIN "Course" crs, "CourseRun" cr, "Semester" s
	WHERE r.course_code = cr.course_code and cr.semesterrunsin = s.semester_id and s.academic_year = acad_year AND s.academic_season = acad_season
	group by r.course_code, course_title

$$ LANGUAGE SQL;

SELECT get_percentage (2010,'winter')
  
  
-- 2.8 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_den_exoun_apofitisei () RETURNS TABLE( am character) AS $$

SELECT distinct s.am
FROM  "Student" s, "Diploma" d,			 	-- apo to diploma blepoume an exei apofitisei kai apo to student pernoume to am
(
	SELECT amka_me_arketes_dm.amka																			--	Kratame ta amka autwn pou exoun arketes dm 
	FROM "Student" s, "Graduation_rules" gr, 
	(
		SELECT amka, sum(units) as sum_units																--	Briskoume to sum olwn twn monadwn ana amka
		FROM
		(
			SELECT distinct r.amka, r.course_code, c.units													--	kratame ola ta ma8hmata pou exoun perasei kai ta units tous
			FROM "Course" c, "Register" r
			WHERE r.register_status = 'pass' and r.course_code = c.course_code AND r.amka IN
			(													
				SELECT amka																					--	bgazontas tous apo ton pinaka olwn twn foititwn menoun mono autoi pou ta exoun perasei
				FROM "Student" s
				WHERE s.amka NOT IN
				(
					SELECT distinct amka																	--	kratame mono ta amka tous 1 fora
					FROM( SELECT get_oblig_courses(s.amka), s.amka FROM "Student" s)AS obligatory_Remain	--	osoi xrwstan upoxrewtika ma8hmata 
				)
			)			
		)AS total_units_table
		GROUP BY amka
	)AS amka_me_arketes_dm
	WHERE sum_units >= gr.min_units and s.amka = amka_me_arketes_dm.amka and EXTRACT(YEAR from s.entry_date) = gr.year_rules  -- gia na sindesoume m tous kanones ths xronias tous

	INTERSECT

	SELECT amka_me_passed_epiloghs.amka																		--	kratame autous pou exoun perasei 14 epiloghs
	FROM "Student" s, "Graduation_rules" gr,
	(
		SELECT g.amka, count(g.amka) as perasmena_epiloghs													--	krataei osous exoun perasei ma8hmata epiloghs kai posa einai auta
		FROM
		(
			SELECT r.amka, r.course_code 
			FROM "Register" r, "Course" c
			WHERE r.course_code = c.course_code and c.obligatory = 'false' and r.register_status = 'pass'
		)as g
		group by g.amka
	)as amka_me_passed_epiloghs
	WHERE perasmena_epiloghs >= gr.min_courses and s.amka = amka_me_passed_epiloghs.amka and EXTRACT(YEAR from s.entry_date) = gr.year_rules
	
)AS etoimoi_gia_apofoitish
WHERE etoimoi_gia_apofoitish.amka = d.amka and s.amka = d.amka and graduation_date IS NULL and thesis_grade IS NOT NULL
-- an exoun hmerominia apofitishs, shmainei oti exoun apofoitisei. AN den exoun ba8mo diplwmatikhs den pliroun tis prohpo8eseis
$$ LANGUAGE SQL;

SELECT get_den_exoun_apofitisei ()	
	
	
--	2.9 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_fortos_erg_pros () 
RETURNS TABLE(amka integer, surname character, name character, wres_ergasias bigint) AS $$	

	SELECT amka, surname, name, sum(lab_hours) as fortos
	FROM (
		SELECT ls.amka, ls.surname, ls.name, c.course_code, c.lab_hours
		FROM "LabStaff" ls, "Lab_fields" lf, "Course" c
		WHERE ls.labworks = lf.lab_code and left(c.course_code, 3) = lf.field_code
	)AS staff_with_courses
	GROUP BY amka, surname, name
	order by amka

$$ LANGUAGE SQL;

SELECT get_fortos_erg_pros ()	


--	2.10 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_prohgoumena (c_code character) 
RETURNS TABLE(code character, title character) AS $$	

	WITH RECURSIVE Req(main ,dependent) AS (
		SELECT main, dependent
		FROM "Course_depends" 
		where dependent = c_code
		UNION
		SELECT d.main, d.dependent
		FROM Req r, "Course_depends" d
		WHERE d.dependent = r.main
	)
	SELECT distinct main, course_title FROM Req, "Course"
	where main = course_code
	order by main

$$ LANGUAGE SQL;

SELECT * from get_prohgoumena ('ΗΡΥ 411')	


--	2.11 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION get_am_me_perasmena_plh () 
RETURNS TABLE( am character, onomatepwnumo character) AS $$	

	select am , name || ' ' || surname
	from "Student" s, (
		SELECT amka, c.course_code, row_number() OVER (PARTITION BY amka ORDER BY amka asc)::integer	-- ari8moume ta ma8hmata ana amka
		FROM "Register" r natural join "Course" c
		WHERE register_status = 'pass' and c.obligatory = 'true' and left(course_code, 3) = 'ΠΛΗ'		-- perasmena registers upoxrwetikwn plh
		order by amka
		)as perasmena_plh
	where row_number = (select count(*) from "Course" where (left(course_code, 3) = 'ΠΛΗ' and obligatory = 'true')) and s.amka = perasmena_plh.amka		
	-- kratame ta registers me row number iso me ton ari8mo twn ipoxrewtikvn plh kai ta sundeoume me tous foitites tous
	
$$ LANGUAGE SQL;

SELECT * from get_am_me_perasmena_plh ()	


-- 3.1 ------------------------------------------------------------------------------------------------------------
-- sunarthsh gia insert sto semester
CREATE OR REPLACE FUNCTION check_sem_ins() RETURNS TRIGGER AS $$

BEGIN
-- start date mesa se allo eksamino
	IF ((select semester_id from (select semester_id, start_date, end_date from "Semester") as zeugaria -- epistrefei to id tou eksaminou pou ginetai h epikalipsi. an einai null den iparxei
	where start_date < new.start_date and end_date > new.start_date)IS NOT NULL) THEN 	-- koitaei an h nea hmeromhnia einai anamesa apo ena opoiodhpote iparxon zeugari start kai end date
		RAISE EXCEPTION 'Epikalipsh eksaminou logo arxikhs hmerominias';
-- end date mesa se allo eksamino		
	ELSIF ((select semester_id from (select semester_id, start_date, end_date from "Semester") as zeugaria -- epistrefei to id tou eksaminou pou ginetai h epikalipsi. an einai null den iparxei
	where start_date < new.end_date and end_date > new.end_date)IS NOT NULL) THEN 	-- koitaei an h nea hmeromhnia einai anamesa apo ena opoiodhpote iparxon zeugari start kai end date
		RAISE EXCEPTION 'Epikalipsh eksaminou logo telikhs hmerominias';
-- epikaliptei toulaxiston 1 eksamino eksoloklhrou			
	ELSIF ((select count(semester_id) from (select semester_id, start_date, end_date from "Semester") as zeugaria -- epistrefei to id tou eksaminou pou ginetai h epikalipsi. an einai null den iparxei
	where start_date > new.start_date and end_date < new.end_date) != 0) THEN 	-- metrame posa epikaliptei, an einai 0 pernaei
		RAISE EXCEPTION 'Epikalipsh eksaminou logo eksoterikhs epikalipsis';
-- bazei past se eksamino pou einai meta to present		
	ELSIF new.semester_status != 'past' and new.end_date < (select start_date from "Semester" where semester_status = 'present') then
		RAISE EXCEPTION  'παραβιάζεται η χρονική σειρά των εξαµήνων';
-- bazei future se eksamino pou einai prin to present			
	ELSIF new.semester_status != 'future' and new.start_date > (select end_date from "Semester" where semester_status = 'present') then
		RAISE EXCEPTION  'παραβιάζεται η χρονική σειρά των εξαµήνων';	
	END IF;

RETURN NEW;
END;

$$ LANGUAGE 'plpgsql';	
-- trigger gia insert sto semester
CREATE TRIGGER check_sem_ins
BEFORE INSERT ON "Semester"
FOR EACH ROW 
EXECUTE PROCEDURE check_sem_ins()


-- sunarthsh gia update sto semester
CREATE OR REPLACE FUNCTION check_sem_upd() RETURNS TRIGGER AS $$
-- de ginontai sugkriseis me ton eauto tous gia na mporoun na allaksoun 
BEGIN
-- start date mesa se allo eksamino
	IF ((select semester_id from (select semester_id, start_date, end_date from "Semester" s where s.semester_id != old.semester_id) as zeugaria -- epistrefei to id tou eksaminou pou ginetai h epikalipsi. an einai null den iparxei
	where start_date < new.start_date and end_date > new.start_date)IS NOT NULL) THEN 
		RAISE EXCEPTION 'Epikalipsh eksaminou logo arxikhs hmerominias';
-- end date mesa se allo eksamino	
	ELSIF ((select semester_id from (select semester_id, start_date, end_date from "Semester"  s where s.semester_id != old.semester_id) as zeugaria 
	where start_date < new.end_date and end_date > new.end_date)IS NOT NULL) THEN 
		RAISE EXCEPTION 'Epikalipsh eksaminou logo telikhs hmerominias';
-- epikaliptei toulaxiston 1 eksamino eksoloklhrou		
	ELSIF ((select count(semester_id) from (select semester_id, start_date, end_date from "Semester"  s where s.semester_id != old.semester_id) as zeugaria
	where start_date > new.start_date and end_date < new.end_date) != 0) THEN
		RAISE EXCEPTION 'Epikalipsh eksaminou logo eksoterikhs epikalipsis';
-- iparxei allo present		
	ELSIF (new.semester_status = 'present' and (select semester_id from "Semester" s where semester_status = 'present' and s.semester_id != old.semester_id) is not null ) then
		RAISE EXCEPTION 'iparxei allo present';
-- bazei past se eksamino pou einai meta to present			
	ELSIF new.semester_status != 'past' and new.end_date < (select start_date from "Semester" s where semester_status = 'present' and s.semester_id != old.semester_id) then
		RAISE EXCEPTION  'παραβιάζεται η χρονική σειρά των εξαµήνων';
-- bazei future se eksamino pou einai prin to present			
	ELSIF new.semester_status != 'future' and new.start_date > (select end_date from "Semester" s where semester_status = 'present' and s.semester_id != old.semester_id) then
		RAISE EXCEPTION  'παραβιάζεται η χρονική σειρά των εξαµήνων';
    ELSIF new.semester_status = 'present' and (select semester_status from "Semester" where semester_id = new.semester_id - 1) = 'future' then
	    RAISE EXCEPTION  'υπάρχει μελλοντικό εξάμηνο πιο πριν ';
	END IF;

RETURN NEW;
END;

$$ LANGUAGE 'plpgsql';
-- trigger gia update sto semester
CREATE TRIGGER check_sem_upd
BEFORE UPDATE ON "Semester"
FOR EACH ROW 
EXECUTE PROCEDURE check_sem_upd()

-- elenxos
update "Semester" set  semester_status = 'present' where semester_id = 22
insert into "Semester" values(25, 2222,'spring', '1040-03-18','1040-03-19','present')

select * from "Register" WHERE serial_number = 22 and register_status != 'rejected' and   register_status != 'proposed'


-- 3.2 ------------------------------------------------------------------------------------------------------------
-- xriazetai na ftiaksoume thn update_grades()
CREATE OR REPLACE FUNCTION update_grades_on_sem_change() RETURNS TRIGGER AS $$
BEGIN
	PERFORM update_grades();
RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- trigger
CREATE TRIGGER semester_change
BEFORE Update ON "Semester"
FOR EACH ROW 
when (old.semester_status = 'present' and new.semester_status = 'past')
EXECUTE PROCEDURE update_grades_on_sem_change()


-- 3.3 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION register_status_change_check() RETURNS TRIGGER AS $$

BEGIN

	if (select sum + units 							-- an me to neo ma8hma oi monades einai parapanw apo 35 den mporei na to parei
	from
	( select distinct  units, amka from "Course" c, "Register" where c.course_code = old.course_code and amka = old.amka  )as neo, 	-- bgazei tis monades tou neou ma8hmatos
	(
		select s.amka, sum(units)					-- ari8mos units ana amka
		from "Student" s left join(					-- gia n a bgalei olous tous ma8tes, kai autous pou dn exoun deilwsei tpt
			select s.amka, c.course_code, c.units, r.register_status	
			from "Register" r natural join "Student" s, "Course" c, "Semester" sem
			where c.course_code = r.course_code and sem.semester_id = r.serial_number and sem.semester_status = 'present' and r.register_status = 'approved'
		)as x on s.amka = x.amka
		group by s.amka
	)deilwmena
	where neo.amka = deilwmena.amka
		
	) > 35 then
		new.register_status := 'rejected';
	   	RAISE NOTICE  'parapanw apo 35 monades'; 
	   
	elsif (select count + 1							-- an me to neo ma8hma einai parapanw apo oxtw den mporei na to parei
	from 
	(
		select s.amka, count(x.amka)				-- ari8mos ma8hmatwn ana amka
		from "Student" s left join(					-- gia n a bgalei olous tous ma8htes, kai autous pou dn exoun deilwsei tpt
			select s.amka, r.course_code, r.register_status	
			from "Register" r natural join "Student" s,  "Semester" sem
			where  sem.semester_id = r.serial_number and sem.semester_status = 'present' and r.register_status = 'approved'
		)as x on s.amka = x.amka
		group by s.amka
	)deilwmena
	where amka = old.amka

	) > 8 then
		new.register_status := 'rejected';
		RAISE NOTICE  'parapanw apo 8 ma8hmata';
	   
	elsif (
	(
		select distinct main from "Course", "Course_depends" 
  		where mode = 'required' and dependent = old.course_code
  	) is not null    								-- briskei an iparxei alisida

	and 
		
	(
		select main									-- briskei an thn exei perasei
		from "Register",(
			select distinct main
			from "Course", "Course_depends" 
			where mode = 'required' and dependent = old.course_code 
		)as alisida
		where main = course_code and amka = old.amka and register_status = 'pass') is null 
	
	) then
		new.register_status := 'rejected';
		RAISE NOTICE  'den exei perasei alisida';
			
	end if;

RETURN NEW;
END;

$$ LANGUAGE 'plpgsql';

CREATE TRIGGER register_status_change
BEFORE Update ON "Register"
for each row
when ((old.register_status = 'proposed' or old.register_status = 'requested') and new.register_status = 'approved')
EXECUTE PROCEDURE register_status_change_check()

select *
from "Register"
where serial_number = 22 and amka = 99

update "Register" set register_status = 'approved' where amka = 99 and serial_number = 22 and course_code = 'ΜΑΘ 302'


-- 4.1 ------------------------------------------------------------------------------------------------------------
CREATE or replace VIEW present_courses_view AS 
select p1.course_code, p1.course_title, concat(p1.surname || ' ' || p1.name, ', ' || p2.surname || ' ' || p2.name)		-- concat giati || me null ta kanei ola null
from(
	SELECT c.course_code, c.course_title, p.name, p.surname			-- ma8hmata me tous prof1
	FROM "Course" c natural join "CourseRun" cr, "Semester" s, "Professor" p
	WHERE cr.semesterrunsin = s.semester_id AND s.semester_status = 'present' and p.amka = amka_prof1
) as p1
left join 
(
	SELECT c.course_code, c.course_title, p.name, p.surname			-- ma8hmata me tous prof2
	FROM "Course" c natural join "CourseRun" cr, "Semester" s, "Professor" p
	WHERE cr.semesterrunsin = s.semester_id AND s.semester_status = 'present' and p.amka = amka_prof2
)as p2
on p1.course_code = p2.course_code									-- sindesh tous

select * from present_courses_view


-- 4.1 ------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION foitites_me_perasmena_prohgoumenou_etous() RETURNS TABLE(amka integer, etos_spoudwn integer) AS $$
-- briskei tous foitites pou exoun perasei ta ipoxrewtika ma8hmata tou prohgoumenou etous
select amka, etos_spoudwn 											-- autoi pou exoun perasei ola ta ipoxrewtika tou prohgoumenou etous
from(
	select amka, count(amka) as perasmena, etos_spoudwn				-- ari8mos perasmenwn ana foitith
	from (
		select amka, course_code, etos_spoudwn						-- perasmena prohgoumenou etous ana foitith
		from "Register" natural join(
			select amka, course_code, etos_spoudwn					-- ipoxrewtika prohgoumenou etous ka8e foitith
			from "Course", (
				select amka, ((select semester_id from "Semester" where semester_status = 'present') - semester_id) / 2 as etos_spoudwn		--etos spoudwn ka8e foitith
				from "Student", "Semester"
				where EXTRACT(YEAR from entry_date) = academic_year and academic_season = 'winter'
			)as foitites
			where typical_year = etos_spoudwn - 1 and obligatory = 'true'
			order by etos_spoudwn, amka
		)as ipoxrewtika
		where register_status = 'pass'
	)as perasmena
	group by amka, etos_spoudwn
)as ari8mos_perasmenwn,
(
	select typical_year, count(typical_year) as ipoxrewtika			--posa ipoxrewtika exei ka8e etos
	from ( select course_code, typical_year from "Course" where obligatory = 'true' )as etos_ma8hmatwn
	group by typical_year
)as ari8mos_ipoxrewtikwn_ana_etos
where perasmena = ipoxrewtika

union

select amka, etos_spoudwn 											-- bazoume kai autous pou einai 6o etos giati sto 5o den ipirxan ipoxrewtika
from (
	select amka, ((select semester_id from "Semester" where semester_status = 'present') - semester_id) / 2 as etos_spoudwn		--etos spoudwn ka8e foitith
	from "Student", "Semester"
	where EXTRACT(YEAR from entry_date) = academic_year and academic_season = 'winter'
)as foitites
where etos_spoudwn = 6

$$
LANGUAGE SQL;	

select * from foitites_me_perasmena_prohgoumenou_etous()


create or replace view ethsios_ba8mos as
select s.amka, name, surname, mesos_oros, etos_spoudwn	-- sundeoume ta onomata tous
from "Student" s,
(
	select distinct amka, etos_spoudwn, sum (weight * final_grade) over (partition by amka) / sum(weight) over (partition by amka) as mesos_oros
	from
	(
		select ft.amka, ft.etos_spoudwn, r.course_code, weight, r.final_grade
		from "Course" natural join "Register" r, foitites_me_perasmena_prohgoumenou_etous() ft
		where ft.amka = r.amka and r.register_status = 'pass' and typical_year = ft.etos_spoudwn - 1
		-- ma8hmata pou exoun oloklhrwsei to prohgoumeno etos oi egkiroi foitites
		order by ft.amka
	)as mesoi_oroi
)as ba8moi
where s.amka = ba8moi.amka



-- 2o meros ===================================================================================================================
-- 1
------------------- εδώ οι συναρτήσεις που δίνονταν στο 4ο εργαστήριο με μία ακόμη για τα πατρώνυμα ----------------
CREATE OR REPLACE FUNCTION random_surnames(n integer)
RETURNS TABLE(surname character(50), id integer) AS
$$
BEGIN
	RETURN QUERY
	SELECT snam.surname, row_number() OVER ()::integer
	FROM (SELECT "Surname".surname 
		FROM "Surname"
		WHERE right("Surname".surname,2)='ΗΣ' 
		ORDER BY random() LIMIT n) as snam;
END;
$$
LANGUAGE 'plpgsql' VOLATILE;

CREATE OR REPLACE FUNCTION random_names(n integer)
RETURNS TABLE(name character(30),sex character(1), id integer) AS
$$
BEGIN
	RETURN QUERY
	SELECT nam.name, nam.sex, row_number() OVER ()::integer
	FROM (SELECT "Name".name, "Name".sex 
		FROM "Name" 
		ORDER BY random() LIMIT n) as nam;
END;
$$
LANGUAGE 'plpgsql' VOLATILE;
  
CREATE OR REPLACE FUNCTION create_am(year integer, num integer)
RETURNS character(10) AS
$$
BEGIN
	RETURN concat(year::character(4),lpad(num::text,6,'0'));
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION adapt_surname(surname character(50), 
sex character(1)) RETURNS character(50) AS
$$
DECLARE
	result character(50);
BEGIN
	result = surname;
	IF right(surname,2)<>'ΗΣ' THEN
		RAISE NOTICE 'Cannot handle this surname';
	ELSIF sex='F' THEN
		result = left(surname,-1);
	ELSIF sex<>'M' THEN
		RAISE NOTICE 'Wrong sex parameter';
	END IF;
	RETURN result;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION random_father_names(n integer)
RETURNS TABLE(father_name character(30), id integer) AS
$$
BEGIN
	RETURN QUERY
	SELECT nam.name as father_name, row_number() OVER ()::integer
	FROM (SELECT "Name".name 
		FROM "Name" 
		WHERE "Name".sex='M'
		ORDER BY random() LIMIT n) as nam;
END;
$$
LANGUAGE 'plpgsql' VOLATILE;

CREATE SEQUENCE IF NOT EXISTS amka_sequence;
ALTER SEQUENCE amka_sequence RESTART WITH 100000;

CREATE OR REPLACE FUNCTION insert_students(year integer, num integer, startam integer)
RETURNS VOID AS
$$
BEGIN
	CREATE SEQUENCE IF NOT EXISTS am_sequence;
	PERFORM setval('am_sequence',startam,false);
		
	INSERT INTO "Student"(amka,name,father_name,surname,email,am,entry_date)
	SELECT nextval('amka_sequence'), n.name, f.father_name,
		adapt_surname(s.surname,n.sex),
		currval('amka_sequence')::text||'@ced.tuc.gr', 
		create_am(year,nextval('am_sequence')::integer), (year::text||'-09-01')::date
	FROM random_names(num) n NATURAL JOIN random_surnames(num) s 
		NATURAL JOIN random_father_names(num) f;
	
END;
$$
LANGUAGE 'plpgsql' VOLATILE;

-- bazei pollous foitites ----------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION public.insert_1000_students_per_year(
    yearstart integer,
    yearfinish integer)
  RETURNS void AS
$BODY$
DECLARE
	year integer;
	round integer;
BEGIN
	FOR year in yearstart..yearfinish LOOP
		FOR round in 1..5 LOOP
			PERFORM insert_students(year,200,200*(round-1)+1);
		END LOOP;
	END LOOP;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
  
SELECT insert_1000_students_per_year(1500,1899);


--2 ----------------------------------------------------------------------------------------------------
-- A
-- explain analize
explain analyze
select * 
from "Student"
where surname >= 'ΜΑ' and surname <= 'ΜΟ'

-- btree index
CREATE INDEX student_surname_idx ON "Student" USING btree(surname);
set enable_seqscan = off;

DROP INDEX student_surname_idx;
set enable_seqscan = on;

-- hash index
CREATE INDEX student_surname_idx ON "Student" USING hash(surname);
set enable_seqscan = off;

DROP INDEX student_surname_idx;
set enable_seqscan = on;

-- cluster 
CLUSTER "Student" USING student_surname_idx;

CLUSTER "Student" USING "Student_pkey"


-- B --------------------------------------------------------------------------------------------
-- elenxos twn pass apeksw
explain analyze
select foithths1.amka, foithths2.amka, foithths1.course_code, foithths1.final_grade
from "Register" as foithths1 join "Register" as foithths2 
on foithths1.amka < foithths2.amka and foithths1.register_status = 'pass'
	and (foithths1.course_code, foithths1.final_grade, foithths1.register_status) 
		= (foithths2.course_code, foithths2.final_grade, foithths2.register_status)

-- enable/disable joins
set enable_hashjoin=on;
set enable_mergejoin=on;
set enable_hashjoin=off;
set enable_mergejoin=off;

-- create indexes
CREATE INDEX register_course_idx ON "Register" USING hash(course_code);
CREATE INDEX register_grade_idx ON "Register" USING hash(final_grade);
CREATE INDEX register_status_idx ON "Register" USING hash(register_status);
CREATE INDEX register_amka_idx ON "Register" USING hash(amka);
CLUSTER "Register" USING register_amka_idx;	-- reduntant

-- clear indexes
DROP INDEX register_course_idx;
DROP INDEX register_grade_idx;
DROP INDEX register_status_idx;
DROP INDEX register_amka_idx;
CLUSTER "Register" USING "Register_pkey"


-- 3 ------------------------------------------------------------------------------------------------------------
CREATE or replace VIEW show_diplomas AS 
select s.am, s.name, s.surname, EXTRACT(YEAR FROM s.entry_date)::character(4) as entry_year, d.thesis_grade, p.name || ' ' || p.surname as onomateponumo_ka8igith
from "Student" s, "Diploma" d, "Professor" p
where s.amka = d.amka and d.amka_super = p.amka;

ALTER TABLE show_diplomas
	OWNER TO postgres;

-- trigger gia update sto view
CREATE OR REPLACE FUNCTION diploma_view_update() RETURNS TRIGGER AS $$
BEGIN
	if old.am != new.am then
		RAISE EXCEPTION  'Απαγορεύεται η αλλαγή αριθμού μητρώου';
	end if;
	-- an skasei to exception den sinexizei	
	-- allages sto Student
	if old.name != new.name or old.surname != new.surname or old.entry_year != new.entry_year then
		update "Student" s 
		set ( name, surname, entry_date ) = 												-- allagh onomatos, epwnumou kai ton xrono sto entry date tou foitith
			( new.name, new.surname, s.entry_date + ( new.entry_year::integer - EXTRACT( YEAR FROM s.entry_date ) || ' years' )::interval ) 
		where s.am = old.am;
	end if;
	
	-- allages sto Diploma
	if old.name != new.name or old.surname != new.surname or old.thesis_grade != new.thesis_grade or old.onomateponumo_ka8igith != new.onomateponumo_ka8igith then
		if new.onomateponumo_ka8igith not in ( select name || ' ' || surname from "Professor" ) then
			raise exception 'Den uparxei ka8hghths me auto to onomateponumo';
		end if;
		update "Diploma" d 
		set ( thesis_grade, thesis_title, amka_super ) = 										-- allagh ba8mou, onomatos ergasias kai amka super
			( new.thesis_grade, 'Διπλωματική εργασία' || rpad( new.name, 30, ' ' ) || new.surname,
			(select amka from "Professor" where ((name || ' ' || surname) = new.onomateponumo_ka8igith) limit 1)) 
		where ( select amka from "Student" s where s.am = old.am ) = d.amka;					-- to diploma exei amka kai prepei na to broume apo to student
	end if;
	
	RETURN NEW;
END;
$$ LANGUAGE 'plpgsql';

-- trigger
CREATE TRIGGER diploma_view_update
instead of Update ON show_diplomas -- epeidh einai view xreiazetai instead of
FOR EACH ROW 
EXECUTE PROCEDURE diploma_view_update()

-- ELENXOI update =======================================================================================================
--  allagh am
update show_diplomas set( am, name ) = ( '1', 'aefea' ) where am = '2010000001'
-- allagh stoixeiwn foitith
update show_diplomas set( name, surname, entry_year ) = ( 'a', 'b', '2000' ) where am = '2010000001'
-- allagh stoixeiwn diplomatos
update show_diplomas set( name, surname, thesis_grade, onomateponumo_ka8igith ) = ( 'c', 'd', 11, 'ΑΜΑΡΥΛΛΙΣ ΜΙΣΧΕΔΑΚΗ' ) where am = '2010000001'
-- allagh se ka8hghth pou den iparxei
update show_diplomas set( name, surname, thesis_grade, onomateponumo_ka8igith ) = ( 'c', 'd', 11, 'ujfvyu ΜΙΣΧΕΔΑΚΗ' ) where am = '2010000001'
-- ELENXOI update =======================================================================================================

-- trigger gia insert sto view
CREATE OR REPLACE FUNCTION diploma_view_insert() RETURNS TRIGGER AS $$
BEGIN
	-- elenxos or8othtas
	if new.onomateponumo_ka8igith not in ( select name || ' ' || surname from "Professor" ) then
		raise exception 'Den uparxei ka8hghths me auto to onomateponumo';
	elsif new.am not in ( select am from "Student") then												-- den uparxei autos o foitiths sth bash
		raise exception 'Den iparxei autos o foitithts';
	end if;
	
	-- periptwseis view
	if new.am in ( select am from show_diplomas ) then													-- iparxei idh sto view
		update show_diplomas 
		set( thesis_grade, onomateponumo_ka8igith ) = ( new.thesis_grade, new.onomateponumo_ka8igith)	-- elenxetai apo to allo trigger
		where am = new.am;
		return null;
	elsif new.am in (select am from "Student") then														-- iparxei sto "Students"
		insert into "Diploma"
		select foitites.amka, new.thesis_grade, ('Διπλωματική εργασία' || rpad( foitites.name, 30, ' ' ) || foitites.surname) as thesis_title, null, null, null, 
			(select amka from "Professor" where ((name || ' ' || surname) = new.onomateponumo_ka8igith) limit 1),
			floor(random() * (max(p.amka) - min(p.amka + 1)) + min(p.amka))::int,
			floor(random() * (max(p.amka) - min(p.amka + 1)) + min(p.amka))::int
		from ( select amka, name, surname from  "Student" where am = new.am )as foitites, "Professor" p
		group by foitites.amka, thesis_title;
		return new;
	end if;
END;
$$ LANGUAGE 'plpgsql';

-- trigger
CREATE TRIGGER diploma_view_insert
instead of insert ON show_diplomas -- epeidh einai view xreiazetai instead of
FOR EACH ROW 
EXECUTE PROCEDURE diploma_view_insert();

-- revoke insert into on (select name, surname, entry_year from show_diplomas) from 
-- ELENXOI insert =======================================================================================================
-- den iparxei o foitiths
insert into show_diplomas(am,thesis_grade,onomateponumo_ka8igith) values ('0',0,'efcewf efwef') 
-- iparxei o foitiths alla la8os onoma ka8hghth
insert into show_diplomas(am,thesis_grade,onomateponumo_ka8igith) values ('2010000001',30,'wafda effef') 

-- iparxei idh mesa sto view
insert into show_diplomas(am,thesis_grade,onomateponumo_ka8igith) values ('2010000001',0,'efcewf efwef')
-- iparxei o foitiths alla den einai sto "Diploma"
insert into show_diplomas(am,thesis_grade,onomateponumo_ka8igith) values ('2017000001',60,'ΑΜΑΡΥΛΛΙΣ ΜΙΣΧΕΔΑΚΗ') 
-- ELENXOI insert =======================================================================================================