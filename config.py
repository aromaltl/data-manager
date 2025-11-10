
annotation_directory = "/home/tl028/SKRPL"
train_ratio = 0.8
csvpath = ""
labelstudiojson= ""
fn_image_path = lambda x: x.replace("/data/local-files/?d=/run/user/1000/gvfs/smb-share:server=anton.local,share=labelstudio/data","/run/user/1000/gvfs/smb-share:server=anton.local,share=sync/Label-studio_backup/data")
query="""
	SELECT
	    i.image_id,
	    i.image_name,
	    i.image_path,
	    i.width as image_width,
	    i.height as image_height,
	    s.site_name,
	    u.email,
	    i.project as project_id,
	    i.created_at,
	    a.x1,
	    a.y1,
	    a.x2,
	    a.y2,
	    c.class_name as classname,
	    m.contour
	FROM images i
	INNER JOIN sites s on i.site_id = s.site_id
	INNER JOIN annotations a ON i.image_id = a.image_id
	INNER JOIN classes c ON a.class_id = c.class_id
	INNER JOIN usr u ON i.user_id = u.user_id
	LEFT JOIN mask m ON a.annotation_id = m.annotation_id
    where i.image_id < 800;
"""

