function Link(el)
  -- Only process external links (http or https)
  if el.target:match("^http") then
    -- Get the visible text of the link
    local link_text = pandoc.utils.stringify(el.content)
    
    -- If the link text is the same as the target URL, skip the footnote
    if link_text == el.target then
      return el
    end

    -- Create a footnote containing the URL
    local footnote = pandoc.Note({pandoc.Plain({pandoc.Str(el.target)})})
    -- Return the original link followed by the footnote
    return {el, footnote}
  end
  return el
end
