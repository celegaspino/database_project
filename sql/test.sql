--test query6
--flight id = 54, departure date = 2014-04-28
--total seats = 247, num sold = 19, available = 228
/*
Select * 
From FlightInfo
Where flight_id = 54;

Select P.seats
From Plane P, FlightInfo I
Where I.plane_id = P.id and I.flight_id = 54;

Select F.num_sold
From Flight F, FlightInfo I
Where F.fnum = I.flight_id and F.fnum = 54;

Select (P.seats - F.num_sold) as Available
From Plane P, Flight F, FlightInfo I
Where P.id = I.plane_id and F.fnum = I.flight_id and F.fnum = 54;

--lists all flights with desired departure date
Select * 
From Flight
Where actual_departure_date::date >= '2014-04-28' and actual_departure_date < ('2014-04-28'::date + '1 day'::interval);
*/

--test query5
/*
Select lname,COUNT(*)
From Customer
Group by lname;
*/


DO $$
BEGIN
IF (Select P.seats From Plane P, FlightInfo I Where I.plane_id = P.id and I.flight_id = 54) > (Select F.num_sold From Flight F, FlightInfo I Where F.fnum = I.flight_id and F.fnum = 54) 
THEN 
	INSERT INTO Reservation (cid, fid, status) VALUES ((Select id From Customer Where fname = 'Ina' and lname = 'Lecroy'), 54, 'C'); 
ELSE 
	INSERT INTO Reservation (cid, fid, status) VALUES ((Select id From Customer Where fname = 'Ina' and lname = 'Lecroy'), 54, 'W'); 
END IF;
END $$;



--INSERT INTO Reservation (cid, fid, status) VALUES (5, 54, 'W');

Select * 
From Reservation;

